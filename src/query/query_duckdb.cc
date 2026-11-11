/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query_duckdb.cc
 *    The C++ adapter to DuckDB: the replacement scan that turns a table
 *    name into partitions and memtable batches, and the check that a
 *    statement only reads what it may.
 *
 * This is the one file that uses DuckDB's C++ API, written against the
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
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
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
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/literal.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include <duckdb.h>

#include "query_priv.h"
#include "schema.h"
#include "ut.h"
#include "ut_err.h"

/** The id counter belongs to the database, not the process.
@return the database-instance address and database-local connection id */
static query_connection_key_t query_context_key(
    duckdb::ClientContext &context) {
  return (std::make_pair(reinterpret_cast<uintptr_t>(context.db.get()),
                         static_cast<uint64_t>(context.GetConnectionId())));
}

/** Get the composite connection identity. */
query_connection_key_t query_connection_key(duckdb_connection conn) {
  duckdb::Connection &connection =
      *reinterpret_cast<duckdb::Connection *>(conn);

  return (query_context_key(*connection.context));
}

/** Replace a table name of a running query by the SELECT over its partitions
and memtable batches. Other names are left to DuckDB, which reports them as
unknown.
@return a generated SELECT subquery for a registered logical table, or NULL to
  leave the name unresolved */
static duckdb::unique_ptr<duckdb::TableRef> query_replace_table(
    duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
    duckdb::optional_ptr<duckdb::ReplacementScanData> data) {
  static_cast<void>(data);

  if (!input.catalog_name.empty() || !input.schema_name.empty()) {
    return (nullptr);
  }

  uint64_t token = 0;
  const query_ranges_t *ranges = nullptr;
  const query_snapshot_t *snap =
      query_find_by_connection(query_context_key(context), token, ranges);

  if (snap == nullptr) {
    return (nullptr);
  }

  ut_a(ranges != nullptr);

  std::string name;
  if (!schema_canonical_table_name(input.table_name.data(),
                                   input.table_name.size(), name)
           .ok()) {
    return (nullptr);
  }

  std::map<std::string, query_table_t>::const_iterator it =
      snap->m_tables.find(name);

  if (it == snap->m_tables.end()) {
    return (nullptr);
  }

  /* Parse the generated SELECT into a subquery bound in place of the
  name. Any error is reported by DuckDB as a query error. */
  query_range_t range;

  query_ranges_t::const_iterator r = ranges->find(name);
  if (r != ranges->end()) {
    range = r->second;
  }

  const std::string sql =
      query_build_table_sql(token, name, it->second, snap->m_partition_days,
                            range, snap->m_flushed_seq);

  duckdb::Parser parser = duckdb::Parser::GetBuiltinParser();
  parser.ParseQuery(sql);

  duckdb::unique_ptr<duckdb::SelectStatement> select =
      duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(
          std::move(parser.statements[0]));

  return (duckdb::make_uniq<duckdb::SubqueryRef>(
      std::move(select), duckdb::Identifier(input.table_name)));
}

/** Register the table replacement callback in the database of a connection. */
void query_register_table_replacement(duckdb_connection conn) {
  duckdb::Connection &connection =
      *reinterpret_cast<duckdb::Connection *>(conn);

  duckdb::DBConfig::GetConfig(*connection.context)
      .replacement_scans.push_back(
          duckdb::ReplacementScan(query_replace_table));
}

/** Check one table reference; set error on the first refusal. */
static db_status_t query_check_ref(duckdb::TableRef &ref) {
  switch (ref.type) {
    case duckdb::TableReferenceType::BASE_TABLE: {
      const duckdb::QualifiedName &qn =
          ref.Cast<duckdb::BaseTableRef>().GetQualifiedName();

      if (!qn.Catalog().empty() || !qn.Schema().empty()) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "qualified table names are not allowed"));
      }

      const std::string &raw = qn.Name().GetIdentifierName();
      std::string name;

      /* An identifier: no file paths or other strings DuckDB would read
      as files. */
      if (!schema_canonical_table_name(raw.data(), raw.size(), name).ok()) {
        return (db_status_t(DB_INVALID_ARGUMENT,
                            "\"" + raw + "\" is not a table name"));
      }

      return (DB_SUCCESS);
    }

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
static db_status_t query_check_read_node(duckdb::QueryNode &node) {
  switch (node.type) {
    case duckdb::QueryNodeType::SELECT_NODE:
      break;

    case duckdb::QueryNodeType::SET_OPERATION_NODE: {
      duckdb::SetOperationNode &setop = node.Cast<duckdb::SetOperationNode>();
      for (duckdb::unique_ptr<duckdb::QueryNode> &child : setop.children) {
        const db_status_t status = query_check_read_node(*child);
        if (!status.ok()) {
          return (status);
        }
      }
      break;
    }

    case duckdb::QueryNodeType::RECURSIVE_CTE_NODE: {
      duckdb::RecursiveCTENode &cte = node.Cast<duckdb::RecursiveCTENode>();
      db_status_t status = query_check_read_node(*cte.left);
      if (!status.ok()) {
        return (status);
      }
      status = query_check_read_node(*cte.right);
      if (!status.ok()) {
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
      const db_status_t status = query_check_read_node(*cte.second->query_node);
      if (!status.ok()) {
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
@param[in]  visit_node  visitor for a pivot-values subquery, if present */
static void query_visit_pivot_children(
    duckdb::TableRef &ref,
    const std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)>
        &visit_expr,
    const std::function<void(duckdb::QueryNode &)> &visit_node) {
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

    if (column.subquery) {
      visit_node(*column.subquery);
    }
  }
}

/** Check every table reference and query node, subqueries included. */
static db_status_t query_check_node(duckdb::QueryNode &node) {
  db_status_t status = query_check_read_node(node);
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
      status = query_check_node(*sub.Subquery()->node);
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
        query_visit_pivot_children(ref, expr, [&](duckdb::QueryNode &child) {
          if (!status.ok()) {
            return;
          }
          status = query_check_node(child);
        });
        if (!status.ok()) {
          return;
        }
        if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
          duckdb::SubqueryRef &sub = ref.Cast<duckdb::SubqueryRef>();
          status = query_check_read_node(*sub.subquery->node);
          if (!status.ok()) {
            return;
          }
        }
        status = query_check_ref(ref);
      });

  return (status);
}

/** Extract bounds from an already parsed SELECT. */
static void query_ranges_from_node(duckdb::QueryNode &node,
                                   query_ranges_t &ranges);

/** Validate and extract ranges from one SELECT after C API syntax checking. */
db_status_t query_validate_select_statement(const std::string &sql,
                                            query_ranges_t &ranges) {
  ranges.clear();
  duckdb::Parser parser = duckdb::Parser::GetBuiltinParser();
  parser.ParseQuery(sql);

  if (parser.statements.size() != 1) {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "exactly one statement is required"));
  }

  if (parser.statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "only SELECT statements are allowed"));
  }

  duckdb::SelectStatement &select =
      parser.statements[0]->Cast<duckdb::SelectStatement>();
  const db_status_t err = query_check_node(*select.node);
  if (!err.ok()) {
    return (err);
  }

  query_ranges_from_node(*select.node, ranges);
  return (DB_SUCCESS);
}

/** Parse and validate ordinary-table DDL after C API syntax checking. */
db_status_t query_check_ddl_statement(const std::string &sql,
                                      query_ddl_t &ddl) {
  duckdb::Parser parser = duckdb::Parser::GetBuiltinParser();
  parser.ParseQuery(sql);
  if (parser.statements.size() != 1) {
    return (
        db_status_t(DB_INVALID_ARGUMENT, "exactly one statement is required"));
  }

  duckdb::SQLStatement &statement = *parser.statements[0];
  const duckdb::QualifiedName *name = nullptr;
  if (statement.type == duckdb::StatementType::CREATE_STATEMENT) {
    duckdb::CreateInfo &base = *statement.Cast<duckdb::CreateStatement>().info;
    if (base.type != duckdb::CatalogType::TABLE_ENTRY || base.temporary ||
        base.internal) {
      return (db_status_t(DB_INVALID_ARGUMENT,
                          "only persistent ordinary tables may be created"));
    }
    duckdb::CreateTableInfo &info = base.Cast<duckdb::CreateTableInfo>();
    for (const duckdb::unique_ptr<duckdb::Constraint> &constraint :
         info.constraints) {
      if (constraint->type == duckdb::ConstraintType::FOREIGN_KEY) {
        return (
            db_status_t(DB_INVALID_ARGUMENT,
                        "foreign keys are not supported for ordinary tables"));
      }
    }
    if (info.query) {
      const db_status_t status = query_check_node(*info.query->node);
      if (!status.ok()) {
        return (status);
      }
    }
    ddl.m_is_create = true;
    name = &info.GetQualifiedName();
  } else if (statement.type == duckdb::StatementType::DROP_STATEMENT) {
    duckdb::DropInfo &info = *statement.Cast<duckdb::DropStatement>().info;
    if (info.type != duckdb::CatalogType::TABLE_ENTRY || info.cascade ||
        info.allow_drop_internal) {
      return (
          db_status_t(DB_INVALID_ARGUMENT,
                      "only ordinary tables may be dropped, without CASCADE"));
    }
    ddl.m_if_exists = info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;
    name = &info.GetQualifiedName();
  } else {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "only CREATE TABLE and DROP TABLE are accepted"));
  }

  if (!name->Catalog().empty() ||
      (!name->Schema().empty() &&
       name->Schema() != duckdb::Identifier::DefaultSchema())) {
    return (db_status_t(DB_INVALID_ARGUMENT,
                        "ordinary tables must be in the main schema"));
  }
  const std::string &raw = name->Name().GetIdentifierName();
  if (!schema_canonical_table_name(raw.data(), raw.size(), ddl.m_table).ok()) {
    return (db_status_t(DB_INVALID_ARGUMENT, "invalid ordinary table name"));
  }

  return (DB_SUCCESS);
}

/** What the time conditions of a statement say about its table references. */
struct query_walk_t {
  /** Every table name reference. */
  std::vector<const duckdb::BaseTableRef *> m_refs;

  /** Ranges of the references that are the whole FROM of a SELECT. */
  std::map<const duckdb::TableRef *, query_range_t> m_bounded;
};

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
  if (!duckdb::StringUtil::CIEquals(column, SCHEMA_TIME_COLUMN)) {
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
static void query_range_narrow(query_range_t &range, duckdb::ExpressionType op,
                               int64_t c) {
  switch (op) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      range.m_lo = std::max(range.m_lo, c);
      range.m_hi = std::min(range.m_hi, c);

      return;

    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      if (c == INT64_MAX) {
        range.m_lo = INT64_MAX;
        range.m_hi = INT64_MIN;

        return;
      }
      range.m_lo = std::max(range.m_lo, c + 1);

      return;

    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      range.m_lo = std::max(range.m_lo, c);

      return;

    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      if (c == INT64_MIN) {
        range.m_lo = INT64_MAX;
        range.m_hi = INT64_MIN;

        return;
      }
      range.m_hi = std::min(range.m_hi, c - 1);

      return;

    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      range.m_hi = std::min(range.m_hi, c);

      return;

    default:
      return;
  }
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
static void query_range_where(const duckdb::ParsedExpression &expr,
                              const std::string &name, const std::string &alias,
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
        query_range_where(*children[i], name, alias, range);
      }

      return;
    }

    case duckdb::ExpressionClass::COMPARISON: {
      const duckdb::ComparisonExpression &cmp =
          expr.Cast<duckdb::ComparisonExpression>();
      const duckdb::ExpressionType op = expr.GetExpressionType();

      if (query_is_time_column(cmp.Left(), name, alias) &&
          query_parse_time_constant(cmp.Right(), c)) {
        query_range_narrow(range, op, c);
      } else if (query_is_time_column(cmp.Right(), name, alias) &&
                 query_parse_time_constant(cmp.Left(), c)) {
        query_range_narrow(range, query_flip_comparison(op), c);
      }

      return;
    }

    case duckdb::ExpressionClass::BETWEEN: {
      const duckdb::BetweenExpression &between =
          expr.Cast<duckdb::BetweenExpression>();

      if (query_is_time_column(between.Input(), name, alias) &&
          query_parse_time_constant(between.LowerBound(), c) &&
          query_parse_time_constant(between.UpperBound(), c2)) {
        query_range_narrow(
            range, duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO, c);
        query_range_narrow(
            range, duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO, c2);
      }

      return;
    }

    default:
      return;
  }
}

/** Find the ranges of the SELECTs a query node is made of: the node itself,
the branches of set operations and recursive CTEs, and its CTEs. SELECTs
inside table references and expressions are reached by the caller's walk. */
static void query_collect_select_ranges(duckdb::QueryNode &node,
                                        query_walk_t &walk) {
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
          query_range_where(*select.where_clause, name,
                            from->alias.GetIdentifierName(), range);
        }

        walk.m_bounded[from] = range;
      }
      break;
    }

    case duckdb::QueryNodeType::SET_OPERATION_NODE: {
      duckdb::SetOperationNode &setop = node.Cast<duckdb::SetOperationNode>();

      for (size_t i = 0; i < setop.children.size(); ++i) {
        query_collect_select_ranges(*setop.children[i], walk);
      }
      break;
    }

    case duckdb::QueryNodeType::RECURSIVE_CTE_NODE: {
      duckdb::RecursiveCTENode &cte = node.Cast<duckdb::RecursiveCTENode>();

      query_collect_select_ranges(*cte.left, walk);
      query_collect_select_ranges(*cte.right, walk);
      break;
    }

    default:
      break;
  }

  for (duckdb::pair<duckdb::Identifier,
                    duckdb::unique_ptr<duckdb::CommonTableExpressionInfo>> &kv :
       node.cte_map.map) {
    if (kv.second->query_node) {
      query_collect_select_ranges(*kv.second->query_node, walk);
    }
  }
}

/** Walk a query node, subqueries included, recording every table reference
and the ranges of the SELECTs. */
static void query_collect_ranges(duckdb::QueryNode &node, query_walk_t &walk) {
  query_collect_select_ranges(node, walk);

  std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)> expr;

  expr = [&](duckdb::unique_ptr<duckdb::ParsedExpression> &child) {
    if (child->GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      duckdb::SubqueryExpression &sub =
          child->Cast<duckdb::SubqueryExpression>();
      query_collect_ranges(*sub.Subquery()->node, walk);
    }

    duckdb::ParsedExpressionIterator::EnumerateChildren(*child, expr);
  };

  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
      node, expr, [&](duckdb::TableRef &ref) {
        query_visit_pivot_children(ref, expr, [&](duckdb::QueryNode &child) {
          query_collect_ranges(child, walk);
        });

        if (ref.type == duckdb::TableReferenceType::BASE_TABLE) {
          walk.m_refs.push_back(&ref.Cast<duckdb::BaseTableRef>());
        } else if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
          query_collect_select_ranges(
              *ref.Cast<duckdb::SubqueryRef>().subquery->node, walk);
        }
      });
}

/** Work out table bounds from a SELECT already checked for read restrictions.
 */
static void query_ranges_from_node(duckdb::QueryNode &node,
                                   query_ranges_t &ranges) {
  query_walk_t walk;
  query_collect_ranges(node, walk);

  /* A table gets the smallest range covering every reference to its name;
  one reference without a range leaves the table unbounded. */
  std::set<std::string> unbounded;
  query_ranges_t found;

  for (size_t i = 0; i < walk.m_refs.size(); ++i) {
    const duckdb::BaseTableRef *ref = walk.m_refs[i];
    const std::string &raw = ref->GetQualifiedName().Name().GetIdentifierName();
    std::string name;

    if (!schema_canonical_table_name(raw.data(), raw.size(), name).ok()) {
      continue;
    }

    std::map<const duckdb::TableRef *, query_range_t>::const_iterator b =
        walk.m_bounded.find(ref);

    if (b == walk.m_bounded.end()) {
      unbounded.insert(name);
      continue;
    }

    const query_range_t &range = b->second;

    query_ranges_t::iterator it = found.find(name);
    if (it == found.end()) {
      found[name] = range;
    } else if (range.m_lo <= range.m_hi) {
      /* An empty range adds nothing. */
      if (it->second.m_lo > it->second.m_hi) {
        it->second = range;
      } else {
        it->second.m_lo = std::min(it->second.m_lo, range.m_lo);
        it->second.m_hi = std::max(it->second.m_hi, range.m_hi);
      }
    }
  }

  for (query_ranges_t::iterator it = found.begin(); it != found.end(); ++it) {
    const bool all =
        it->second.m_lo == INT64_MIN && it->second.m_hi == INT64_MAX;

    if (!all && unbounded.count(it->first) == 0) {
      ranges[it->first] = it->second;
    }
  }
}
