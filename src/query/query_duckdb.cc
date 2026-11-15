/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_duckdb.cc
 *    The C++ adapter to DuckDB: table replacement, read-only statement
 *    analysis, and plan retention within a query's fixed snapshot.
 *
 * This adapter uses DuckDB's C++ API against the
 * pinned DuckDB commit. It reaches the database behind a C API handle
 * through DuckDB's internal C API wrapper, and it follows DuckDB's own
 * conventions where its API requires them: DuckDB's smart pointers, and
 * lambdas for the std::function callbacks of its tree walker. Upgrading
 * DuckDB means checking this file first.
 *
 *-------------------------------------------------------------------------
 */

#include "query.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/capi/capi_internal_table.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/literal.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_trigger_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/detach_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include <duckdb.h>

#include "db_schema.h"
#include "pt_duckdb.h"
#include "ut.h"

#ifdef PT_TESTING
#include "test_query_access.h"
#endif /* PT_TESTING */

/** Read the native byte count before it is rounded for display by SQL. */
uint64_t pt_duckdb_memory_limit_bytes(duckdb_connection connection) {
  const auto *native = reinterpret_cast<duckdb::Connection *>(connection);

  return (duckdb::DBConfig::GetConfig(*native->context).options.maximum_memory);
}

/** The id counter belongs to the database, not the process.
@return the database-instance address and database-local connection id */
static query_connection_key_t query_context_key(
    duckdb::ClientContext &context) {
  return (std::make_pair(reinterpret_cast<uintptr_t>(context.db.get()),
                         static_cast<uint64_t>(context.GetConnectionId())));
}

/** Authorize plan retention only while preparing this registered snapshot.
The connection remains in the same explicit transaction until its cursor closes.
Nested preparations restore their caller's scope on every return. */
class query_prepare_scope final : private ut_noncopyable {
 public:
  /** Enter a preparation without extending the lifetime of its snapshot. */
  query_prepare_scope(duckdb_connection connection, uint64_t token,
                      const query_snapshot_t &snapshot)
      : m_connection_key(query_connection_key(connection)),
        m_token(token),
        m_snapshot(&snapshot),
        m_previous(m_current) {
    duckdb::ClientContext &context =
        *reinterpret_cast<duckdb::Connection *>(connection)->context;

    if (context.transaction.HasActiveTransaction() &&
        !context.transaction.IsAutoCommit()) {
      m_transaction = &context.transaction.ActiveTransaction();
    }

    m_current = this;
  }

  /** Restore an enclosing preparation, if any. */
  ~query_prepare_scope() { m_current = m_previous; }

  /** Check connection, transaction and registered snapshot identity. */
  static bool matches(duckdb::ClientContext &context) {
    const query_prepare_scope *scope = m_current;

    /* Only the connection preparing this registered query may retain a plan. */
    if (scope == nullptr ||
        scope->m_connection_key != query_context_key(context)) {
      return (false);
    }

    /* The cursor must still own the same valid explicit transaction. */
    if (scope->m_transaction == nullptr ||
        !context.transaction.HasActiveTransaction() ||
        context.transaction.IsAutoCommit()) {
      return (false);
    }

    duckdb::MetaTransaction &transaction =
        context.transaction.ActiveTransaction();
    if (scope->m_transaction != &transaction ||
        duckdb::ValidChecker::IsInvalidated(transaction)) {
      return (false);
    }

    /* Registration must still identify the exact snapshot being prepared. */
    uint64_t token = 0;
    const query_ranges_t *ranges = nullptr;
    const query_snapshot_t *snapshot =
        query_registry::instance().find_by_connection(scope->m_connection_key,
                                                      token, ranges);

    return (token != 0 && token == scope->m_token &&
            snapshot == scope->m_snapshot && ranges != nullptr);
  }

 private:
  /** Innermost preparation on this thread; never retained by a plan. */
  static thread_local const query_prepare_scope *m_current;

  /** Database-local connection identity. */
  const query_connection_key_t m_connection_key;

  /** Registration of the query being prepared. */
  const uint64_t m_token;

  /** Snapshot retained by the owning database cursor. */
  const query_snapshot_t *m_snapshot;

  /** Explicit transaction already established before preparing. */
  const duckdb::MetaTransaction *m_transaction{nullptr};

  /** Enclosing preparation restored on exit. */
  const query_prepare_scope *m_previous;
};

/** A copyable plan binding sharing only immutable C bind data.
DuckDB requires this inheritance and these virtual methods. Each scan still
creates independent execution state through the original C init callback. */
class query_memtable_binding final : public duckdb::CTableBindData,
                                     private ut_noncopyable {
 public:
  /** Share the original owner and copy the current plan's metadata. */
  query_memtable_binding(std::shared_ptr<duckdb::CTableBindData> owner,
                         const duckdb::CTableBindData &source)
      : CTableBindData(source.info), m_owner(std::move(owner)) {
    bind_data = m_owner->bind_data;

    /* Only m_owner invokes the C deletion callback. */
    delete_callback = nullptr;
    column_ids = source.column_ids;
    if (source.stats) {
      stats = duckdb::make_uniq<duckdb::NodeStatistics>(*source.stats);
    }
  }

  /** Eligibility was established at bind time, not at execution time. */
  bool SupportStatementCache() const override { return (true); }

  /** Copy plan metadata without sharing a scan's mutable progress. */
  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override {
    return (duckdb::make_uniq<query_memtable_binding>(m_owner, *this));
  }

  /** Only copies of the same immutable binding and projection are equal. */
  bool Equals(const duckdb::FunctionData &other) const override {
    const query_memtable_binding *binding =
        dynamic_cast<const query_memtable_binding *>(&other);

    return (binding != nullptr && m_owner == binding->m_owner &&
            column_ids == binding->column_ids);
  }

 private:
  friend void query_wrap_memtable_bind(duckdb_table_function function);
#ifdef PT_TESTING
  friend class query_bind_counter;
#endif /* PT_TESTING */

  /** Wrap bindings only inside the registered preparation scope. */
  static duckdb::unique_ptr<duckdb::FunctionData> bind(
      duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
      duckdb::vector<duckdb::LogicalType> &types,
      duckdb::vector<duckdb::Identifier> &names);

  /* ================== Atomic callback registration ================== */

  /** Fixed C API trampoline, checked once across database registrations. */
  static std::atomic<duckdb::table_function_bind_t> m_original_bind;

  /* ================== Immutable binding ownership =================== */

  /** Last wrapper releases the original C bind object and its callback. */
  const std::shared_ptr<duckdb::CTableBindData> m_owner;
};

/** Get the composite connection identity. */
query_connection_key_t query_connection_key(duckdb_connection conn) {
  duckdb::Connection &connection =
      *reinterpret_cast<duckdb::Connection *>(conn);

  return (query_context_key(*connection.context));
}

thread_local const query_prepare_scope *query_prepare_scope::m_current =
    nullptr;

/** Fixed C API trampoline, installed once and checked across databases. */
std::atomic<duckdb::table_function_bind_t>
    query_memtable_binding::m_original_bind{nullptr};

#ifdef PT_TESTING
query_bind_counter *query_bind_counter::m_current = nullptr;

/** Count one bind call before forwarding it to DuckDB. */
duckdb::unique_ptr<duckdb::FunctionData> query_bind_counter::count_bind(
    duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &types,
    duckdb::vector<duckdb::Identifier> &names) {
  ++m_current->m_calls;

  return (m_current->m_original(context, input, types, names));
}

/** Install a bind counter while retaining the original DuckDB callback. */
query_bind_counter::query_bind_counter() {
  ut_a(m_current == nullptr);

  m_current = this;
  m_original = query_memtable_binding::m_original_bind.exchange(count_bind);
  ut_a(m_original != nullptr);
}

/** Restore the original bind callback when counting ends. */
query_bind_counter::~query_bind_counter() {
  query_memtable_binding::m_original_bind.store(m_original);
  m_current = nullptr;
}

/** Create a binding without exposing the adapter's implementation type. */
duckdb::unique_ptr<duckdb::FunctionData> query_test_create_binding(
    std::shared_ptr<duckdb::CTableBindData> owner,
    const duckdb::CTableBindData &source) {
  return (duckdb::make_uniq<query_memtable_binding>(std::move(owner), source));
}

/** Enter a preparation scope for the supplied snapshot. */
query_test_scope::query_test_scope(duckdb_connection connection, uint64_t token,
                                   const query_snapshot_t &snapshot)
    : m_scope(ut_new<query_prepare_scope>(connection, token, snapshot)) {
  ut_a(m_scope != nullptr);
}

/** Leave the scope and restore any enclosing preparation. */
query_test_scope::~query_test_scope() {
  ut_delete(m_scope);
}

/** Check whether this connection matches the active preparation. */
bool query_test_scope::matches(duckdb_connection connection) {
  return (query_prepare_scope::matches(
      *reinterpret_cast<duckdb::Connection *>(connection)->context));
}
#endif /* PT_TESTING */

/** Wrap only bindings created for the current query's fixed snapshot. */
duckdb::unique_ptr<duckdb::FunctionData> query_memtable_binding::bind(
    duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
    duckdb::vector<duckdb::LogicalType> &types,
    duckdb::vector<duckdb::Identifier> &names) {
  duckdb::unique_ptr<duckdb::FunctionData> original =
      m_original_bind.load()(context, input, types, names);
  if (!query_prepare_scope::matches(context)) {
    return (original);
  }

  std::shared_ptr<duckdb::CTableBindData> owner(
      static_cast<duckdb::CTableBindData *>(original.release()));

  return (duckdb::make_uniq<query_memtable_binding>(owner, *owner));
}

/** Install the adapter before the function is published to any connection. */
void query_wrap_memtable_bind(duckdb_table_function function) {
  duckdb::TableFunction &native =
      *reinterpret_cast<duckdb::TableFunction *>(function);
  duckdb::table_function_bind_t expected = nullptr;
  if (!query_memtable_binding::m_original_bind.compare_exchange_strong(
          expected, native.bind)) {
    ut_a(expected == native.bind);
  }

  native.bind = query_memtable_binding::bind;
}

/** Remove only the optimizer's conservative data-source rebind decision.
A binder-required rebind already removed the child plan. This pre-optimizer
hook leaves later optimizer decisions and execution-time validity checks intact.
*/
static void query_retain_snapshot_plan(
    duckdb::OptimizerExtensionInput &input,
    duckdb::unique_ptr<duckdb::LogicalOperator> &plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_PREPARE ||
      !query_prepare_scope::matches(input.context)) {
    return;
  }

  duckdb::LogicalPrepare &prepare = plan->Cast<duckdb::LogicalPrepare>();
  if (prepare.children.size() == 1 &&
      prepare.prepared->properties.bound_all_parameters) {
    prepare.prepared->properties.always_require_rebind = false;
  }
}

/** Prepare once within the cursor's exclusive connection and fixed transaction.
The handle belongs to this cursor and is executed once, never across queries. */
duckdb_state query_prepare(duckdb_connection connection, const char *sql,
                           uint64_t token, const query_snapshot_t &snapshot,
                           duckdb_prepared_statement *out) {
  query_prepare_scope scope(connection, token, snapshot);

  return (duckdb_prepare(connection, sql, out));
}

/** Add an exec hint only after DuckDB fails to resolve a table normally. */
class query_exec_diagnostics final : private ut_noncopyable {
 public:
  /** Bind diagnostics to this connection and the caller's time-series names. */
  query_exec_diagnostics(duckdb_connection connection,
                         const std::set<std::string> &tables)
      : m_context(*reinterpret_cast<duckdb::Connection *>(connection)->context),
        m_tables(tables),
        m_catalog(duckdb::ClientData::Get(m_context)
                      .default_database.GetIdentifierName()),
        m_previous(m_current) {
    m_current = this;
  }

  /** Restore the enclosing call, if any. */
  ~query_exec_diagnostics() { m_current = m_previous; }

  /** Reset before each statement so a later error cannot inherit its hint. */
  void reset() { m_missing.clear(); }

  /** Retain native diagnostics and resource/cancellation error categories. */
  db_status_t failure(db_status_t error) const {
    if (error.code() != DB_ERROR || m_missing.empty()) {
      return (error);
    }

    return (error.with_context("cannot read time-series table '" + m_missing +
                               "' through exec; use the query API"));
  }

  /** Record unresolved names only in the active exec connection. */
  static void missing(duckdb::ClientContext &context,
                      const duckdb::ReplacementScanInput &input) {
    query_exec_diagnostics *diagnostics = m_current;
    if (diagnostics == nullptr || &diagnostics->m_context != &context) {
      return;
    }

    const std::string name = duckdb::StringUtil::Lower(input.table_name);
    if (diagnostics->m_tables.count(name) == 0) {
      return;
    }

    const duckdb::CatalogSearchPath &path =
        *duckdb::ClientData::Get(context).catalog_search_path;
    const auto matches = [&](const std::string &catalog) {
      return (duckdb::StringUtil::CIEquals(catalog, diagnostics->m_catalog));
    };

    if (!input.catalog_name.empty()) {
      if (!matches(input.catalog_name)) {
        return;
      }

      if (!input.schema_name.empty() &&
          !duckdb::StringUtil::CIEquals(input.schema_name, "main")) {
        return;
      }
    } else if (!input.schema_name.empty()) {
      if (!matches(input.schema_name)) {
        if (!duckdb::StringUtil::CIEquals(input.schema_name, "main") ||
            !matches(path.ResolveCatalog(duckdb::Identifier("main"))
                         .GetIdentifierName())) {
          return;
        }
      }
    } else if (!path.SchemaInSearchPath(
                   context, duckdb::Identifier(diagnostics->m_catalog),
                   duckdb::Identifier("main"))) {
      return;
    }

    diagnostics->m_missing = name;
  }

 private:
  /** Active exec on this binding thread; never retained in a plan. */
  static thread_local query_exec_diagnostics *m_current;

  /** Connection whose native lookup is being observed. */
  duckdb::ClientContext &m_context;

  /** Published time-series names for this call. */
  const std::set<std::string> &m_tables;

  /** PulseTen catalog, retained across USE. */
  const std::string m_catalog;

  /** Last unresolved time-series name in this statement. */
  std::string m_missing;

  /** Enclosing scope restored on exit. */
  query_exec_diagnostics *m_previous;
};

thread_local query_exec_diagnostics *query_exec_diagnostics::m_current =
    nullptr;

/** Replace a time-series table by its partitions and memtable batches.
Ordinary tables are resolved by DuckDB.
@return a generated SELECT subquery for a registered logical table, or NULL to
  leave the name unresolved */
static duckdb::unique_ptr<duckdb::TableRef> query_replace_table(
    duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
    duckdb::optional_ptr<duckdb::ReplacementScanData> data) {
  static_cast<void>(data);

  query_exec_diagnostics::missing(context, input);

  if (!input.catalog_name.empty()) {
    return (nullptr);
  }

  if (!input.schema_name.empty() &&
      !duckdb::StringUtil::CIEquals(input.schema_name, "main")) {
    return (nullptr);
  }

  uint64_t token = 0;
  const query_ranges_t *ranges = nullptr;
  const query_snapshot_t *snapshot =
      query_registry::instance().find_by_connection(query_context_key(context),
                                                    token, ranges);

  if (snapshot == nullptr) {
    return (nullptr);
  }

  ut_a(ranges != nullptr);

  std::string name;
  if (!schema_normalize_table_name(input.table_name.data(),
                                   input.table_name.size(), name)
           .ok()) {
    return (nullptr);
  }

  std::map<std::string, query_table_t>::const_iterator it =
      snapshot->m_tables.find(name);

  if (it == snapshot->m_tables.end()) {
    return (nullptr);
  }

  /* Leave the name unresolved; the query reports the recorded failure. */
  if (it->second.m_schema.m_columns.empty()) {
    const db_status_t uninitialized(
        DB_TABLE_NOT_INITIALIZED,
        "time-series table schema is not initialized: " + name);
    query_registry::instance().record_error(token, uninitialized);

    return (nullptr);
  }

  /* Parse the generated SELECT into a subquery bound in place of the
  name. Any error is reported by DuckDB as a query error. */
  query_range_t range;

  query_ranges_t::const_iterator r = ranges->find(name);
  if (r != ranges->end()) {
    range = r->second;
  }

  const std::string sql = query_build_scan_sql(token, name, it->second, range,
                                               snapshot->m_flushed_seq);

  duckdb::Parser parser = duckdb::Parser::GetBuiltinParser();
  parser.ParseQuery(sql);

  duckdb::unique_ptr<duckdb::SelectStatement> select =
      duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(
          std::move(parser.statements[0]));

  return (duckdb::make_uniq<duckdb::SubqueryRef>(
      std::move(select), duckdb::Identifier(input.table_name)));
}

/** Install table replacement and plan retention before the database is used. */
void query_register_callbacks(duckdb_connection conn) {
  duckdb::Connection &connection =
      *reinterpret_cast<duckdb::Connection *>(conn);

  duckdb::DBConfig::GetConfig(*connection.context)
      .replacement_scans.push_back(
          duckdb::ReplacementScan(query_replace_table));

  duckdb::OptimizerExtension extension;
  extension.pre_optimize_function = query_retain_snapshot_plan;
  duckdb::OptimizerExtension::Register(
      duckdb::DBConfig::GetConfig(*connection.context), std::move(extension));

  return;
}

/** Extract a possible time-series name from the default schema. */
static bool query_time_series_name(const duckdb::QualifiedName &qualified,
                                   std::string &name) {
  if (!qualified.Catalog().empty()) {
    return (false);
  }

  if (!qualified.Schema().empty() &&
      !duckdb::StringUtil::CIEquals(qualified.Schema().GetIdentifierName(),
                                    "main")) {
    return (false);
  }

  const std::string &raw = qualified.Name().GetIdentifierName();

  return (schema_normalize_table_name(raw.data(), raw.size(), name).ok());
}

/** Validate a table reference against the supported query sources. */
static db_status_t query_validate_table_ref(duckdb::TableRef &ref) {
  switch (ref.type) {
    case duckdb::TableReferenceType::TABLE_FUNCTION:
    case duckdb::TableReferenceType::SHOW_REF:
    case duckdb::TableReferenceType::COLUMN_DATA:
    case duckdb::TableReferenceType::DELIM_GET:
    case duckdb::TableReferenceType::BOUND_TABLE_REF:
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "table functions and system objects cannot be read"));

    default:
      /* Joins, subqueries, VALUES, CTE references, empty FROM. */
      return (DB_SUCCESS);
  }
}

/** Only read-only query nodes may occur in CTEs and set operations. DuckDB's
expression iterator visits their contents without a node callback, so checking
only the outer SELECT statement misses COPY and DML CTEs. */
static db_status_t query_validate_read_only_node(duckdb::QueryNode &node) {
  switch (node.type) {
    case duckdb::QueryNodeType::SELECT_NODE:
      break;

    case duckdb::QueryNodeType::SET_OPERATION_NODE: {
      duckdb::SetOperationNode &setop = node.Cast<duckdb::SetOperationNode>();

      for (duckdb::unique_ptr<duckdb::QueryNode> &child : setop.children) {
        if (const db_status_t status = query_validate_read_only_node(*child);
            !status.ok()) {
          return (status);
        }
      }

      break;
    }

    case duckdb::QueryNodeType::RECURSIVE_CTE_NODE: {
      duckdb::RecursiveCTENode &cte = node.Cast<duckdb::RecursiveCTENode>();
      if (const db_status_t status = query_validate_read_only_node(*cte.left);
          !status.ok()) {
        return (status);
      }

      if (const db_status_t status = query_validate_read_only_node(*cte.right);
          !status.ok()) {
        return (status);
      }

      break;
    }

    default:
      return (db_status_t(
          DB_INVALID_ARGUMENT,
          "only read-only SELECT queries are allowed, including in CTEs"));
  }

  for (duckdb::pair<duckdb::Identifier,
                    duckdb::unique_ptr<duckdb::CommonTableExpressionInfo>>
           &cte : node.cte_map.map) {
    if (cte.second->query_node) {
      if (const db_status_t status =
              query_validate_read_only_node(*cte.second->query_node);
          !status.ok()) {
        return (status);
      }
    }
  }

  return (DB_SUCCESS);
}

/** Visit the PIVOT/UNPIVOT children omitted by DuckDB's table-ref iterator.
The pinned iterator visits the source and aggregates, but not the FOR
expressions or UNPIVOT entries. Both validation and range collection must
visit those children before a table reference can be treated as checked.
A pivot-values subquery never reaches here: the parser moves it into a
generated CREATE TYPE ... AS ENUM statement, checked as its own statement. */
static void query_visit_pivot_children(
    duckdb::TableRef &ref,
    const std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)>
        &visit_expr) {
  if (ref.type != duckdb::TableReferenceType::PIVOT) {
    return;
  }

  duckdb::PivotRef &pivot = ref.Cast<duckdb::PivotRef>();

  for (duckdb::PivotColumn &column : pivot.pivots) {
    for (duckdb::unique_ptr<duckdb::ParsedExpression> &expression :
         column.pivot_expressions) {
      visit_expr(expression);
    }

    for (duckdb::PivotColumnEntry &entry : column.entries) {
      if (entry.expr) {
        visit_expr(entry.expr);
      }
    }
  }

  return;
}

/** Validate query nodes and collect source table names, subqueries included. */
static db_status_t query_analyze_node(duckdb::QueryNode &node,
                                      std::set<std::string> &tables) {
  db_status_t status = query_validate_read_only_node(node);
  if (!status.ok()) {
    return (status);
  }

  std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)> expr;
  expr = [&](duckdb::unique_ptr<duckdb::ParsedExpression> &child) {
    if (!status.ok()) {
      return;
    }

    if (child->GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      duckdb::SubqueryExpression &sub =
          child->Cast<duckdb::SubqueryExpression>();
      status = query_analyze_node(*sub.Subquery()->node, tables);
      if (!status.ok()) {
        return;
      }
    }

    duckdb::ParsedExpressionIterator::EnumerateChildren(*child, expr);
  };

  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
      node, expr, [&](duckdb::TableRef &ref) {
        if (!status.ok()) {
          return;
        }

        query_visit_pivot_children(ref, expr);

        /* A FOR expression may already have failed. */
        if (!status.ok()) {
          return;
        }

        if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
          duckdb::SubqueryRef &sub = ref.Cast<duckdb::SubqueryRef>();
          status = query_validate_read_only_node(*sub.subquery->node);
          if (!status.ok()) {
            return;
          }
        }

        status = query_validate_table_ref(ref);
        if (status.ok() && ref.type == duckdb::TableReferenceType::BASE_TABLE) {
          std::string name;
          if (query_time_series_name(
                  ref.Cast<duckdb::BaseTableRef>().GetQualifiedName(), name)) {
            tables.insert(name);
          }
        }
      });

  return (status);
}

/** Collect conservative time bounds for every table reference. */
class query_range_collector final : private ut_noncopyable {
 public:
  /** Walk one validated SELECT and publish its table bounds. */
  void collect(duckdb::QueryNode &node, query_ranges_t &ranges);

 private:
  /** Record the ranges directly implied by SELECT predicates. */
  void collect_select_ranges(duckdb::QueryNode &node);

  /** Visit a query and its nested table references. */
  void collect_ranges(duckdb::QueryNode &node);

  /** Every table name reference. */
  std::vector<const duckdb::BaseTableRef *> m_table_refs;

  /** Ranges of the references that are the whole FROM of a SELECT. */
  std::map<const duckdb::TableRef *, query_range_t> m_table_ranges;
};

/** Check SQL objects against the namespace managed by PulseTen. */
class query_sql_validator final : private ut_noncopyable {
 public:
  /** Use the connection's search path and the fixed time-series names. */
  query_sql_validator(duckdb_connection conn,
                      const std::set<std::string> &tables)
      : m_context(*reinterpret_cast<duckdb::Connection *>(conn)->context),
        m_tables(tables),
        m_catalog(duckdb::ClientData::Get(m_context)
                      .default_database.GetIdentifierName()) {}

  /** Validate a statement before DuckDB binds or executes it. */
  db_status_t validate(duckdb::SQLStatement &statement);

 private:
  /** Reject internal identifiers and relation names reserved by time series. */
  db_status_t target(const duckdb::QualifiedName &name, bool relation = true,
                     bool schema = false);

  /** Reject whole-database copies into PulseTen storage. */
  db_status_t copy_target(const std::string &catalog);

  /** Check targets nested in CTEs, expressions and table references. */
  db_status_t node(duckdb::QueryNode &query);

  /** Check expression subqueries, including macro bodies. */
  db_status_t expression(duckdb::ParsedExpression &expr);

  /** Reject a foreign key dependency on an internal table. */
  db_status_t constraint(duckdb::Constraint &item);

  /** Check a CREATE target and its source query or indexed table. */
  db_status_t create(duckdb::CreateInfo &info);

  /** Check both the original object and a new name. */
  db_status_t alter(duckdb::AlterInfo &info);

  /** Connection owned by the SQL call. */
  duckdb::ClientContext &m_context;

  /** Published time-series names; namespace changes keep this set stable. */
  const std::set<std::string> &m_tables;

  /** Original database, retained across USE and search-path changes. */
  const std::string m_catalog;
};

/** Resolve the main namespace; time-series rules never reach app tables. */
db_status_t query_sql_validator::target(const duckdb::QualifiedName &name,
                                        bool relation, bool schema) {
  for (const duckdb::Identifier &part : name.Path()) {
    const std::string text =
        duckdb::StringUtil::Lower(part.GetIdentifierName());
    if (text.starts_with(SCHEMA_RESERVED_PREFIX)) {
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "internal object is reserved: " + text));
    }
  }

  const std::string object = duckdb::StringUtil::Lower(
      (name.Name().empty() ? name.Schema() : name.Name()).GetIdentifierName());
  const duckdb::CatalogSearchPath &path =
      *duckdb::ClientData::Get(m_context).catalog_search_path;
  const duckdb::Identifier &catalog =
      name.Catalog().empty() ? path.GetDefault().GetCatalog() : name.Catalog();
  const bool catalog_alias = name.Catalog().empty() &&
                             duckdb::StringUtil::CIEquals(
                                 name.Schema().GetIdentifierName(), m_catalog);
  const bool owned =
      catalog_alias || catalog.empty() ||
      duckdb::StringUtil::CIEquals(catalog.GetIdentifierName(), m_catalog);

  if (schema && owned && object == "main") {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "main schema is managed by PulseTen"));
  }

  if (!relation || !owned) {
    return (DB_SUCCESS);
  }

  const duckdb::Identifier &schema_name =
      name.Schema().empty() ? path.GetDefault().GetSchema() : name.Schema();
  if ((duckdb::StringUtil::CIEquals(schema_name.GetIdentifierName(), "main") ||
       (name.Catalog().empty() &&
        duckdb::StringUtil::CIEquals(schema_name.GetIdentifierName(),
                                     m_catalog))) &&
      m_tables.count(object) != 0) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "time-series table is reserved: " + object));
  }

  return (DB_SUCCESS);
}

/** Walk nested DML as well as the outer query. */
db_status_t query_sql_validator::node(duckdb::QueryNode &query) {
  db_status_t err;
  if (query.type == duckdb::QueryNodeType::INSERT_QUERY_NODE) {
    err = target(query.Cast<duckdb::InsertQueryNode>().qualified_name);
  }

  for (auto &[name, cte] : query.cte_map.map) {
    if (!err.ok()) {
      return (err);
    }

    if (cte->query_node) {
      err = node(*cte->query_node);
    }
  }

  if (!err.ok()) {
    return (err);
  }

  if (query.type == duckdb::QueryNodeType::SET_OPERATION_NODE) {
    for (duckdb::unique_ptr<duckdb::QueryNode> &child :
         query.Cast<duckdb::SetOperationNode>().children) {
      if (const db_status_t status = node(*child); !status.ok()) {
        return (status);
      }
    }
  }

  std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)> expr;
  expr = [&](duckdb::unique_ptr<duckdb::ParsedExpression> &child) {
    if (!err.ok()) {
      return;
    }

    if (child->GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      err = node(*child->Cast<duckdb::SubqueryExpression>().Subquery()->node);
    }

    duckdb::ParsedExpressionIterator::EnumerateChildren(*child, expr);
  };

  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
      query, expr, [&](duckdb::TableRef &ref) {
        if (!err.ok()) {
          return;
        }

        if (ref.type == duckdb::TableReferenceType::BASE_TABLE) {
          err = target(ref.Cast<duckdb::BaseTableRef>().GetQualifiedName(),
                       false);
        } else if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
          err = node(*ref.Cast<duckdb::SubqueryRef>().subquery->node);
        }

        query_visit_pivot_children(ref, expr);
      });

  return (err);
}

/** Inspect subqueries without treating literal text as SQL identifiers. */
db_status_t query_sql_validator::expression(duckdb::ParsedExpression &expr) {
  db_status_t err;
  duckdb::ParsedExpressionIterator::VisitExpressionClassMutable(
      expr, duckdb::ExpressionClass::SUBQUERY,
      [&](duckdb::ParsedExpression &child) {
        if (err.ok()) {
          err =
              node(*child.Cast<duckdb::SubqueryExpression>().Subquery()->node);
        }
      });

  return (err);
}

/** Foreign keys must not make retention depend on ordinary objects. */
db_status_t query_sql_validator::constraint(duckdb::Constraint &item) {
  if (item.type == duckdb::ConstraintType::FOREIGN_KEY) {
    const duckdb::ForeignKeyInfo &key =
        item.Cast<duckdb::ForeignKeyConstraint>().info;

    return (target(
        duckdb::QualifiedName(duckdb::Identifier(), key.schema, key.table)));
  }

  return (DB_SUCCESS);
}

/** Check created names before considering any source query. */
db_status_t query_sql_validator::create(duckdb::CreateInfo &info) {
  const duckdb::QualifiedName &name = info.GetQualifiedName();
  const bool relation = info.type == duckdb::CatalogType::TABLE_ENTRY ||
                        info.type == duckdb::CatalogType::VIEW_ENTRY;
  if (const db_status_t err = target(
          name, relation, info.type == duckdb::CatalogType::SCHEMA_ENTRY);
      !err.ok()) {
    return (err);
  }

  if (info.type == duckdb::CatalogType::TABLE_ENTRY) {
    duckdb::CreateTableInfo &table = info.Cast<duckdb::CreateTableInfo>();
    for (duckdb::unique_ptr<duckdb::Constraint> &item : table.constraints) {
      if (const db_status_t err = constraint(*item); !err.ok()) {
        return (err);
      }
    }

    if (table.query) {
      return (node(*table.query->node));
    }
  } else if (info.type == duckdb::CatalogType::VIEW_ENTRY) {
    return (node(*info.Cast<duckdb::CreateViewInfo>().query->node));
  } else if (info.type == duckdb::CatalogType::INDEX_ENTRY) {
    return (target(name.WithName(info.Cast<duckdb::CreateIndexInfo>().table)));
  } else if (info.type == duckdb::CatalogType::TYPE_ENTRY) {
    duckdb::CreateTypeInfo &type = info.Cast<duckdb::CreateTypeInfo>();
    if (type.query) {
      return (validate(*type.query));
    }
  }

  if (info.type == duckdb::CatalogType::TRIGGER_ENTRY) {
    duckdb::CreateTriggerInfo &trigger = info.Cast<duckdb::CreateTriggerInfo>();
    if (const db_status_t err = target(trigger.base_table->GetQualifiedName());
        !err.ok()) {
      return (err);
    }

    return (node(*trigger.trigger_action));
  }

  if (info.type == duckdb::CatalogType::MACRO_ENTRY ||
      info.type == duckdb::CatalogType::TABLE_MACRO_ENTRY) {
    for (duckdb::unique_ptr<duckdb::MacroFunction> &macro :
         info.Cast<duckdb::CreateMacroInfo>().macros) {
      const db_status_t err =
          macro->type == duckdb::MacroType::TABLE_MACRO
              ? node(*macro->Cast<duckdb::TableMacroFunction>().query_node)
              : expression(
                    *macro->Cast<duckdb::ScalarMacroFunction>().expression);
      if (!err.ok()) {
        return (err);
      }
    }
  }

  return (DB_SUCCESS);
}

/** Check mutation targets, including the destination of a rename. */
db_status_t query_sql_validator::alter(duckdb::AlterInfo &info) {
  const duckdb::QualifiedName &name = info.GetQualifiedName();
  const bool relation =
      info.GetCatalogType() == duckdb::CatalogType::TABLE_ENTRY ||
      info.GetCatalogType() == duckdb::CatalogType::VIEW_ENTRY;
  if (const db_status_t err =
          target(name, relation, info.type == duckdb::AlterType::ALTER_SCHEMA);
      !err.ok()) {
    return (err);
  }

  if (info.type == duckdb::AlterType::ALTER_DATABASE) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "database alteration is not supported by exec"));
  }

  if (info.type == duckdb::AlterType::ALTER_TABLE &&
      info.Cast<duckdb::AlterTableInfo>().alter_table_type ==
          duckdb::AlterTableType::RENAME_TABLE) {
    return (target(
        name.WithName(info.Cast<duckdb::RenameTableInfo>().new_table_name)));
  }

  if (info.type == duckdb::AlterType::ALTER_VIEW &&
      info.Cast<duckdb::AlterViewInfo>().alter_view_type ==
          duckdb::AlterViewType::RENAME_VIEW) {
    return (target(
        name.WithName(info.Cast<duckdb::RenameViewInfo>().new_view_name)));
  }

  if (info.type == duckdb::AlterType::ALTER_TABLE &&
      info.Cast<duckdb::AlterTableInfo>().alter_table_type ==
          duckdb::AlterTableType::ADD_CONSTRAINT) {
    return (constraint(*info.Cast<duckdb::AddConstraintInfo>().constraint));
  }

  return (DB_SUCCESS);
}

/** Use the same catalog comparison for direct COPY and its PRAGMA form. */
db_status_t query_sql_validator::copy_target(const std::string &catalog) {
  if (duckdb::StringUtil::CIEquals(catalog, m_catalog)) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "cannot copy a database into PulseTen storage"));
  }

  return (DB_SUCCESS);
}

/** Dispatch by parsed operation; ordinary SQL execution remains in DuckDB. */
db_status_t query_sql_validator::validate(duckdb::SQLStatement &statement) {
  using duckdb::StatementType;
  switch (statement.type) {
    case StatementType::MULTI_STATEMENT:
      for (duckdb::unique_ptr<duckdb::SQLStatement> &child :
           statement.Cast<duckdb::MultiStatement>().statements) {
        if (const db_status_t err = validate(*child); !err.ok()) {
          return (err);
        }
      }

      return (DB_SUCCESS);
    case StatementType::CREATE_STATEMENT:
      return (create(*statement.Cast<duckdb::CreateStatement>().info));
    case StatementType::ALTER_STATEMENT:
      return (alter(*statement.Cast<duckdb::AlterStatement>().info));
    case StatementType::DROP_STATEMENT: {
      duckdb::DropInfo &info = *statement.Cast<duckdb::DropStatement>().info;
      const bool relation = info.type == duckdb::CatalogType::TABLE_ENTRY ||
                            info.type == duckdb::CatalogType::VIEW_ENTRY;

      return (target(info.GetQualifiedName(), relation,
                     info.type == duckdb::CatalogType::SCHEMA_ENTRY));
    }
    case StatementType::SELECT_STATEMENT:
      return (node(*statement.Cast<duckdb::SelectStatement>().node));
    case StatementType::INSERT_STATEMENT:
      return (node(*statement.Cast<duckdb::InsertStatement>().node));
    case StatementType::UPDATE_STATEMENT:
      return (node(*statement.Cast<duckdb::UpdateStatement>().node));
    case StatementType::DELETE_STATEMENT:
      return (node(*statement.Cast<duckdb::DeleteStatement>().node));
    case StatementType::MERGE_INTO_STATEMENT:
      return (node(*statement.Cast<duckdb::MergeIntoStatement>().node));
    case StatementType::COPY_STATEMENT: {
      duckdb::CopyInfo &info = *statement.Cast<duckdb::CopyStatement>().info;
      if (const db_status_t err = target(info.GetQualifiedName()); !err.ok()) {
        return (err);
      }

      return (info.select_statement ? node(*info.select_statement)
                                    : db_status_t(DB_SUCCESS));
    }
    case StatementType::PREPARE_STATEMENT:
      return (validate(*statement.Cast<duckdb::PrepareStatement>().statement));
    case StatementType::EXECUTE_STATEMENT: {
      duckdb::identifier_map_t<
          duckdb::shared_ptr<duckdb::PreparedStatementData>> &prepared =
          duckdb::ClientData::Get(m_context).prepared_statements;
      const auto found =
          prepared.find(statement.Cast<duckdb::ExecuteStatement>().name);
      if (found != prepared.end() && found->second->unbound_statement) {
        return (validate(*found->second->unbound_statement));
      }

      return (DB_SUCCESS);
    }
    case StatementType::EXPLAIN_STATEMENT:
      return (validate(*statement.Cast<duckdb::ExplainStatement>().stmt));
    case StatementType::ATTACH_STATEMENT:
      if (statement.Cast<duckdb::AttachStatement>().info->on_conflict ==
          duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "database replacement is not supported by exec"));
      }

      return (DB_SUCCESS);
    case StatementType::DETACH_STATEMENT:
      if (duckdb::StringUtil::CIEquals(statement.Cast<duckdb::DetachStatement>()
                                           .info->name.GetIdentifierName(),
                                       m_catalog)) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "cannot detach the PulseTen database"));
      }

      return (DB_SUCCESS);
    case StatementType::COPY_DATABASE_STATEMENT:
      return (copy_target(statement.Cast<duckdb::CopyDatabaseStatement>()
                              .to_database.GetIdentifierName()));
    case StatementType::PRAGMA_STATEMENT: {
      const duckdb::PragmaInfo &info =
          *statement.Cast<duckdb::PragmaStatement>().info;
      const std::string &name = info.name.GetIdentifierName();
      if (duckdb::StringUtil::CIEquals(name, "import_database")) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "database import is not supported by exec"));
      }

      if (!duckdb::StringUtil::CIEquals(name, "copy_database")) {
        return (DB_SUCCESS);
      }

      /* PRAGMA expands into SQL after this check. Inspect only literals,
      without binding or evaluating either argument. */
      if (info.parameters.size() != 2 || !info.named_parameters.empty()) {
        return (
            db_status_t(DB_INVALID_ARGUMENT,
                        "copy_database requires two string literal arguments"));
      }

      for (const duckdb::unique_ptr<duckdb::ParsedExpression> &parameter :
           info.parameters) {
        if (parameter->GetExpressionClass() !=
                duckdb::ExpressionClass::CONSTANT ||
            parameter->Cast<duckdb::ConstantExpression>().GetLiteral().kind !=
                duckdb::LiteralKind::STRING) {
          return (db_status_t(
              DB_INVALID_ARGUMENT,
              "copy_database requires two string literal arguments"));
        }
      }

      return (copy_target(info.parameters[1]
                              ->Cast<duckdb::ConstantExpression>()
                              .GetLiteral()
                              .text));
    }
    default:
      return (DB_SUCCESS);
  }
}

/** Execute one statement and release its native handles on every outcome. */
db_status_t query_command::execute_statement(idx_t index) {
  duckdb_prepared_statement prepared = nullptr;
  duckdb_pending_result pending = nullptr;
  duckdb_result result{};
  db_status_t status;

  /* Prepare the statement without parsing SQL again. */
  if (duckdb_prepare_extracted_statement(m_connection, m_statements, index,
                                         &prepared) != DuckDBSuccess) {
    status = pt_duckdb_error(duckdb_prepare_error(prepared));
  } else if (duckdb_pending_prepared(prepared, &pending) != DuckDBSuccess) {
    status = pt_duckdb_error(duckdb_pending_error(pending));
  } else {
    /* Forward cancellation only after execution has been initialized. */
    if (m_cancel != nullptr && m_cancel->requested()) {
      duckdb_interrupt(m_connection);
    }

    /* Copy execution errors before releasing the result. */
    if (duckdb_execute_pending(pending, &result) != DuckDBSuccess) {
      status = pt_duckdb_error(duckdb_result_error_type(&result),
                               duckdb_result_error(&result));
    }
  }

  /* Release handles on success and failure. */
  duckdb_destroy_result(&result);
  duckdb_destroy_pending(&pending);
  duckdb_destroy_prepare(&prepared);

  return (status);
}

/** Identify statements that need the namespace lock. */
static bool query_changes_namespace(const duckdb::SQLStatement &statement) {
  using duckdb::StatementType;

  switch (statement.type) {
    case StatementType::SELECT_STATEMENT:
    case StatementType::INSERT_STATEMENT:
    case StatementType::UPDATE_STATEMENT:
    case StatementType::DELETE_STATEMENT:
    case StatementType::MERGE_INTO_STATEMENT:
    case StatementType::COPY_STATEMENT:
    case StatementType::TRANSACTION_STATEMENT:
      return (false);
    case StatementType::SET_STATEMENT: {
      const duckdb::SetStatement &setting =
          statement.Cast<duckdb::SetStatement>();
      if (setting.scope == duckdb::SetScope::VARIABLE) {
        return (false);
      }

      /* Built-in SET/RESET, including USE, leave table names unchanged.
      Extension settings may load code or run callbacks; keep them locked. */
      return (duckdb::DBConfig::GetOptionByName(setting.name) == nullptr);
    }
    case StatementType::PREPARE_STATEMENT:
      return (query_changes_namespace(
          *statement.Cast<duckdb::PrepareStatement>().statement));
    case StatementType::EXPLAIN_STATEMENT:
      return (query_changes_namespace(
          *statement.Cast<duckdb::ExplainStatement>().stmt));
    case StatementType::EXECUTE_STATEMENT:
      /* The connection is fresh: every usable PREPARE is in this same call,
      and its body is classified even if EXECUTE appears before it. */
      return (false);
    default:
      /* DDL and other control or extension statements remain serialized. */
      return (true);
  }
}

/** Release native resources before the caller releases its namespace lock. */
query_command::~query_command() {
  duckdb_destroy_extracted(&m_statements);

  if (m_cancel != nullptr) {
    m_cancel->detach();
  }

  duckdb_disconnect(&m_connection);
}

/** Connect, attach cancellation and classify the complete parsed call. */
db_status_t query_command::init(duckdb_database database,
                                const std::string &sql, query_cancel *cancel) {
  ut_ad(m_connection == nullptr && m_statements == nullptr);

  if (cancel != nullptr && cancel->requested()) {
    return (DB_INTERRUPTED);
  }

  /* Open a connection for this SQL command. */
  if (duckdb_connect(database, &m_connection) != DuckDBSuccess) {
    return (db_status_t(DB_ERROR, "cannot connect to DuckDB"));
  }

  /* Bind the connection for cancellation. */
  m_cancel = cancel;
  if (m_cancel != nullptr) {
    m_cancel->attach(m_connection);
  }

  /* Parse SQL once and reuse the statements in execute(). */
  const idx_t count =
      duckdb_extract_statements(m_connection, sql.c_str(), &m_statements);

  /* Check for SQL parsing errors. */
  const char *message = duckdb_extract_statements_error(m_statements);
  if (message != nullptr) {
    return (pt_duckdb_error(message));
  }

  /* If any statement may change names, the caller must acquire the namespace
  lock before executing the first statement. */
  const auto *parsed =
      reinterpret_cast<duckdb::ExtractStatementsWrapper *>(m_statements);
  for (idx_t i = 0; i < count; ++i) {
    if (query_changes_namespace(*parsed->statements[i])) {
      m_changes_namespace = true;
      break;
    }
  }

  return (DB_SUCCESS);
}

/** Validate and execute each parsed statement using the same connection. */
db_status_t query_command::execute(const std::set<std::string> &tables) {
  auto *parsed =
      reinterpret_cast<duckdb::ExtractStatementsWrapper *>(m_statements);

  const idx_t count = parsed->statements.size();

  /* Check SQL for reserved names. */
  query_sql_validator validator(m_connection, tables);

  /* Explain failed time-series reads. */
  query_exec_diagnostics diagnostics(m_connection, tables);

  for (idx_t i = 0; i < count; ++i) {
    if (m_cancel != nullptr && m_cancel->requested()) {
      return (DB_INTERRUPTED);
    }

    /* Validate against the session state left by earlier statements. */
    if (const db_status_t err = validator.validate(*parsed->statements[i]);
        !err.ok()) {
      return (err);
    }

    /* Clear diagnostics from the previous statement. */
    diagnostics.reset();

    if (const db_status_t err = execute_statement(i); !err.ok()) {
      return (diagnostics.failure(err));
    }
  }

  return (DB_SUCCESS);
}

/** Analyze parsed SQL while the caller owns the extracted statements. */
static db_status_t query_analyze_statements(
    duckdb_extracted_statements statements, idx_t count, query_ranges_t &ranges,
    std::vector<std::string> &tables) {
  const char *message = duckdb_extract_statements_error(statements);
  if (count == 0 && message != nullptr) {
    const db_status_t error = pt_duckdb_error(message);

    /* Reject bad syntax while preserving resource failure categories. */
    return (error.code() == DB_ERROR ? error.with_code(DB_INVALID_ARGUMENT)
                                     : error);
  }

  if (count != 1) {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "exactly one statement is required"));
  }

  auto *parsed =
      reinterpret_cast<duckdb::ExtractStatementsWrapper *>(statements);
  duckdb::SQLStatement &statement = *parsed->statements[0];
  if (statement.type != duckdb::StatementType::SELECT_STATEMENT) {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "only SELECT statements are allowed"));
  }

  duckdb::SelectStatement &select = statement.Cast<duckdb::SelectStatement>();

  std::set<std::string> names;
  if (const db_status_t err = query_analyze_node(*select.node, names);
      !err.ok()) {
    return (err);
  }

  query_range_collector().collect(*select.node, ranges);
  tables.assign(names.begin(), names.end());

  return (DB_SUCCESS);
}

/** Parse once, analyze the same syntax tree, then release its C API handle. */
db_status_t query_analyze(duckdb_connection conn, const std::string &sql,
                          query_ranges_t &ranges,
                          std::vector<std::string> &tables) {
  ranges.clear();
  tables.clear();

  duckdb_extracted_statements statements = nullptr;
  const idx_t count = duckdb_extract_statements(conn, sql.c_str(), &statements);
  const db_status_t status =
      query_analyze_statements(statements, count, ranges, tables);
  duckdb_destroy_extracted(&statements);

  return (status);
}

/** Check whether an expression names this table's time column.
@param[in]  name   table name used when there is no alias
@param[in]  alias  table alias; empty when the table has no alias
@return true if an expression is the time column of the table reference with
  this name and alias */
static bool query_is_time_column(const duckdb::ParsedExpression &expr,
                                 const std::string &name,
                                 const std::string &alias) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::COLUMN_REF) {
    return (false);
  }

  const std::vector<duckdb::Identifier> &names =
      expr.Cast<duckdb::ColumnRefExpression>().ColumnNames();

  const std::string &column = names.back().GetIdentifierName();
  if (!duckdb::StringUtil::CIEquals(column, SCHEMA_TIME_COLUMN_NAME)) {
    return (false);
  }

  if (names.size() == 1) {
    return (true);
  }

  /* Qualified by the alias if there is one, else by the table name. */
  const std::string &qualifier = names[0].GetIdentifierName();

  return (names.size() == 2 && duckdb::StringUtil::CIEquals(
                                   qualifier, alias.empty() ? name : alias));
}

/** Read a constant the way DuckDB compares it with a TIMESTAMP: a string
literal, TIMESTAMP '...' or DATE '...'.
@return false if the expression is not such a constant */
static bool query_parse_time_constant(const duckdb::ParsedExpression &expr,
                                      int64_t &us) {
  const duckdb::ParsedExpression *value = &expr;
  bool is_date = false;

  if (expr.GetExpressionClass() == duckdb::ExpressionClass::CAST) {
    const duckdb::CastExpression &cast = expr.Cast<duckdb::CastExpression>();
    const duckdb::TypeExpression &type = cast.TargetType();

    if (cast.IsTryCast() || !type.GetCatalog().empty() ||
        !type.GetSchema().empty() || !type.GetChildren().empty()) {
      return (false);
    }

    const std::string &type_name = type.GetTypeName().GetIdentifierName();

    if (duckdb::StringUtil::CIEquals(type_name, "date")) {
      is_date = true;
    } else if (!duckdb::StringUtil::CIEquals(type_name, "timestamp")) {
      return (false);
    }

    value = &cast.Child();
  }

  if (value->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
    return (false);
  }

  const duckdb::Literal &literal =
      value->Cast<duckdb::ConstantExpression>().GetLiteral();

  if (literal.kind != duckdb::LiteralKind::STRING) {
    return (false);
  }

  /* A DATE compares as midnight of its day. */
  duckdb::Value v = duckdb::Value(literal.text);

  if (is_date) {
    duckdb::optional<duckdb::Value> date =
        v.DefaultTryCastAs(duckdb::LogicalType::DATE);
    if (!date) {
      return (false);
    }

    v = *date;
  }

  duckdb::optional<duckdb::Value> ts =
      v.DefaultTryCastAs(duckdb::LogicalType::TIMESTAMP);
  if (!ts) {
    return (false);
  }

  us = ts->GetValue<duckdb::timestamp_t>().value;

  return (true);
}

/** Narrow a range by "time <op> c".
@param[in,out]  range  inclusive timestamp range to intersect with the
                       comparison
@param[in]      c      constant timestamp in microseconds */
static void query_narrow_range(query_range_t &range, duckdb::ExpressionType op,
                               int64_t c) {
  switch (op) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      range.m_start_time_us = std::max(range.m_start_time_us, c);
      range.m_end_time_us = std::min(range.m_end_time_us, c);

      return;

    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      if (c == INT64_MAX) {
        range.m_start_time_us = INT64_MAX;
        range.m_end_time_us = INT64_MIN;

        return;
      }

      range.m_start_time_us = std::max(range.m_start_time_us, c + 1);

      return;

    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      range.m_start_time_us = std::max(range.m_start_time_us, c);

      return;

    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      if (c == INT64_MIN) {
        range.m_start_time_us = INT64_MAX;
        range.m_end_time_us = INT64_MIN;

        return;
      }

      range.m_end_time_us = std::min(range.m_end_time_us, c - 1);

      return;

    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      range.m_end_time_us = std::min(range.m_end_time_us, c);

      return;

    default:
      return;
  }

  return;
}

/** Reverse the sides of a comparison operator.
@return a comparison with its sides swapped: c < time is time > c */
static duckdb::ExpressionType query_flip_comparison(duckdb::ExpressionType op) {
  switch (op) {
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return (duckdb::ExpressionType::COMPARE_LESSTHAN);

    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return (duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO);

    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      return (duckdb::ExpressionType::COMPARE_GREATERTHAN);

    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return (duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO);

    default:
      return (op);
  }
}

/** Narrow a range by one condition of a WHERE clause. Conditions joined by
AND each narrow it; any other condition is ignored, which only keeps the range
wider than needed.
@param[in]      alias  table alias; empty when the table has no alias
@param[in,out]  range  inclusive time range narrowed by supported conditions */
static void query_apply_time_filter(const duckdb::ParsedExpression &expr,
                                    const std::string &name,
                                    const std::string &alias,
                                    query_range_t &range) {
  int64_t c = 0;
  int64_t c2 = 0;

  switch (expr.GetExpressionClass()) {
    case duckdb::ExpressionClass::CONJUNCTION: {
      if (expr.GetExpressionType() != duckdb::ExpressionType::CONJUNCTION_AND) {
        return;
      }

      const duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>>
          &children = expr.Cast<duckdb::ConjunctionExpression>().GetChildren();

      for (size_t i = 0; i < children.size(); ++i) {
        query_apply_time_filter(*children[i], name, alias, range);
      }

      return;
    }

    case duckdb::ExpressionClass::COMPARISON: {
      const duckdb::ComparisonExpression &cmp =
          expr.Cast<duckdb::ComparisonExpression>();
      const duckdb::ExpressionType op = expr.GetExpressionType();

      if (query_is_time_column(cmp.Left(), name, alias) &&
          query_parse_time_constant(cmp.Right(), c)) {
        query_narrow_range(range, op, c);
      } else if (query_is_time_column(cmp.Right(), name, alias) &&
                 query_parse_time_constant(cmp.Left(), c)) {
        query_narrow_range(range, query_flip_comparison(op), c);
      }

      return;
    }

    case duckdb::ExpressionClass::BETWEEN: {
      const duckdb::BetweenExpression &between =
          expr.Cast<duckdb::BetweenExpression>();

      if (query_is_time_column(between.Input(), name, alias) &&
          query_parse_time_constant(between.LowerBound(), c) &&
          query_parse_time_constant(between.UpperBound(), c2)) {
        query_narrow_range(
            range, duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO, c);
        query_narrow_range(
            range, duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO, c2);
      }

      return;
    }

    default:
      return;
  }

  return;
}

/** Find the ranges of the SELECTs a query node is made of: the node itself,
the branches of set operations and recursive CTEs, and its CTEs. SELECTs
inside table references and expressions are reached by the caller's walk. */
void query_range_collector::collect_select_ranges(duckdb::QueryNode &node) {
  switch (node.type) {
    case duckdb::QueryNodeType::SELECT_NODE: {
      duckdb::SelectNode &select = node.Cast<duckdb::SelectNode>();
      duckdb::TableRef *from = select.from_table.get();

      /* Column aliases can rename the real time column, or name an
      unrelated column "time". Sampling must also see the whole input
      before WHERE is applied. Leave those references unbounded. */
      if (from != nullptr &&
          from->type == duckdb::TableReferenceType::BASE_TABLE &&
          from->Cast<duckdb::BaseTableRef>().column_name_alias.empty() &&
          !select.sample && !from->sample) {
        const std::string &name = from->Cast<duckdb::BaseTableRef>()
                                      .GetQualifiedName()
                                      .Name()
                                      .GetIdentifierName();
        query_range_t range;

        if (select.where_clause) {
          query_apply_time_filter(*select.where_clause, name,
                                  from->alias.GetIdentifierName(), range);
        }

        m_table_ranges[from] = range;
      }

      break;
    }

    case duckdb::QueryNodeType::SET_OPERATION_NODE: {
      duckdb::SetOperationNode &setop = node.Cast<duckdb::SetOperationNode>();

      for (size_t i = 0; i < setop.children.size(); ++i) {
        collect_select_ranges(*setop.children[i]);
      }

      break;
    }

    case duckdb::QueryNodeType::RECURSIVE_CTE_NODE: {
      duckdb::RecursiveCTENode &cte = node.Cast<duckdb::RecursiveCTENode>();

      collect_select_ranges(*cte.left);
      collect_select_ranges(*cte.right);
      break;
    }

    default:
      break;
  }

  for (duckdb::pair<duckdb::Identifier,
                    duckdb::unique_ptr<duckdb::CommonTableExpressionInfo>> &kv :
       node.cte_map.map) {
    if (kv.second->query_node) {
      collect_select_ranges(*kv.second->query_node);
    }
  }

  return;
}

/** Walk a query node, subqueries included, recording every table reference
and the ranges of the SELECTs. */
void query_range_collector::collect_ranges(duckdb::QueryNode &node) {
  collect_select_ranges(node);

  std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)> expr;

  expr = [&](duckdb::unique_ptr<duckdb::ParsedExpression> &child) {
    if (child->GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      duckdb::SubqueryExpression &sub =
          child->Cast<duckdb::SubqueryExpression>();
      collect_ranges(*sub.Subquery()->node);
    }

    duckdb::ParsedExpressionIterator::EnumerateChildren(*child, expr);
  };

  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
      node, expr, [&](duckdb::TableRef &ref) {
        query_visit_pivot_children(ref, expr);

        if (ref.type == duckdb::TableReferenceType::BASE_TABLE) {
          m_table_refs.push_back(&ref.Cast<duckdb::BaseTableRef>());
        } else if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
          collect_select_ranges(
              *ref.Cast<duckdb::SubqueryRef>().subquery->node);
        }
      });

  return;
}

/** Work out table bounds from a SELECT that passed the read checks. */
void query_range_collector::collect(duckdb::QueryNode &node,
                                    query_ranges_t &ranges) {
  m_table_refs.clear();
  m_table_ranges.clear();
  collect_ranges(node);

  /* A table gets the smallest range covering every reference to its name;
  one reference without a range leaves the table unbounded. */
  std::set<std::string> unbounded;
  query_ranges_t found;

  for (size_t i = 0; i < m_table_refs.size(); ++i) {
    const duckdb::BaseTableRef *ref = m_table_refs[i];
    std::string name;

    if (!query_time_series_name(ref->GetQualifiedName(), name)) {
      continue;
    }

    std::map<const duckdb::TableRef *, query_range_t>::const_iterator b =
        m_table_ranges.find(ref);

    if (b == m_table_ranges.end()) {
      unbounded.insert(name);
      continue;
    }

    const query_range_t &range = b->second;

    query_ranges_t::iterator it = found.find(name);

    if (it == found.end()) {
      found[name] = range;
    } else if (range.m_start_time_us <= range.m_end_time_us) {
      /* An empty range adds nothing. */
      if (it->second.m_start_time_us > it->second.m_end_time_us) {
        it->second = range;
      } else {
        it->second.m_start_time_us =
            std::min(it->second.m_start_time_us, range.m_start_time_us);
        it->second.m_end_time_us =
            std::max(it->second.m_end_time_us, range.m_end_time_us);
      }
    }
  }

  for (query_ranges_t::iterator it = found.begin(); it != found.end(); ++it) {
    const bool all = it->second.m_start_time_us == INT64_MIN &&
                     it->second.m_end_time_us == INT64_MAX;

    if (!all && unbounded.count(it->first) == 0) {
      ranges[it->first] = it->second;
    }
  }

  return;
}
