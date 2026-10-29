/*-------------------------------------------------------------------------
 *
 * Copyright (c) 2026, The PulseTen Authors
 * SPDX-License-Identifier: Apache-2.0
 *
 * query0duck.cc
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

#include "query0priv.h"

#include <algorithm>
#include <cstring>
#include <set>

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

/** The id counter belongs to the database, not the process.
@param[in]  context  client context whose database identity and connection id
                     are read
@return the database-instance address and database-local connection id */
static query_connection_key_t query_context_key(
    duckdb::ClientContext &context) {
  return (std::make_pair(reinterpret_cast<uintptr_t>(context.db.get()),
                         (uint64_t)context.GetConnectionId()));
}

/** Get the composite connection identity.
@param[in]  conn  DuckDB connection
@return the database-instance address and database-local connection id */
query_connection_key_t query_connection_key(duckdb_connection conn) {
  duckdb::Connection &connection =
      *reinterpret_cast<duckdb::Connection *>(conn);

  return (query_context_key(*connection.context));
}

/** Replace a table name of a running query by the SELECT over its partitions
and memtable batches. Other names are left to DuckDB, which reports them as
unknown.
@param[in]  context  DuckDB client context for the current query
@param[in]  input    unresolved table reference supplied by DuckDB
@param[in]  data     replacement-scan registration data; unused by this
                     callback
@return a generated SELECT subquery for a registered logical table, or NULL to
  leave the name unresolved */
static duckdb::unique_ptr<duckdb::TableRef> query_replace(
    duckdb::ClientContext &context, duckdb::ReplacementScanInput &input,
    duckdb::optional_ptr<duckdb::ReplacementScanData> data) {
  (void)data;

  if (!input.catalog_name.empty() || !input.schema_name.empty()) {
    return (nullptr);
  }

  uint64_t token = 0;
  const query_ranges_t *ranges = nullptr;
  const query_snapshot_t *snap =
      query_find_by_connection(query_context_key(context), token, &ranges);

  if (snap == nullptr) {
    return (nullptr);
  }

  std::string name;
  if (schema_table_name(input.table_name.data(), input.table_name.size(),
                        name) != DB_SUCCESS) {
    return (nullptr);
  }

  std::map<std::string, query_table_t>::const_iterator it =
      snap->tables.find(name);

  if (it == snap->tables.end()) {
    return (nullptr);
  }

  /* Parse the generated SELECT into a subquery bound in place of the
  name. Any error is reported by DuckDB as a query error. */
  query_range_t range;

  query_ranges_t::const_iterator r = ranges->find(name);
  if (r != ranges->end()) {
    range = r->second;
  }

  const std::string sql = query_table_sql(
      token, name, it->second, snap->partition_days, range, snap->flushed_seq);

  duckdb::Parser parser = duckdb::Parser::GetBuiltinParser();
  parser.ParseQuery(sql);

  duckdb::unique_ptr<duckdb::SelectStatement> select =
      duckdb::unique_ptr_cast<duckdb::SQLStatement, duckdb::SelectStatement>(
          std::move(parser.statements[0]));

  return (duckdb::make_uniq<duckdb::SubqueryRef>(
      std::move(select), duckdb::Identifier(input.table_name)));
}

/** Register the replacement scan with the database.
@param[in,out]  db  DuckDB database receiving the replacement-scan callback */
void query_install_replacement(duckdb_database db) {
  duckdb::DatabaseWrapper *wrapper =
      reinterpret_cast<duckdb::DatabaseWrapper *>(db);
  duckdb::DBConfig &config =
      duckdb::DBConfig::GetConfig(*wrapper->database->instance);

  config.replacement_scans.push_back(duckdb::ReplacementScan(query_replace));
}

/** Check one table reference; set error on the first refusal.
@param[in]      ref    table reference to validate
@param[in,out]  error  first rejection reason; an existing reason is preserved
*/
static void query_check_ref(duckdb::TableRef &ref, std::string &error) {
  if (!error.empty()) {
    return;
  }

  switch (ref.type) {
    case duckdb::TableReferenceType::BASE_TABLE: {
      const duckdb::QualifiedName &qn =
          ref.Cast<duckdb::BaseTableRef>().GetQualifiedName();

      if (!qn.Catalog().empty() || !qn.Schema().empty()) {
        error = "qualified table names are not allowed";

        return;
      }

      const std::string &raw = qn.Name().GetIdentifierName();
      std::string name;

      /* An identifier: no file paths or other strings DuckDB would read
      as files. */
      if (schema_table_name(raw.data(), raw.size(), name) != DB_SUCCESS) {
        error = "\"" + raw + "\" is not a table name";

        return;
      }

      return;
    }

    case duckdb::TableReferenceType::TABLE_FUNCTION:
    case duckdb::TableReferenceType::SHOW_REF:
    case duckdb::TableReferenceType::COLUMN_DATA:
    case duckdb::TableReferenceType::DELIM_GET:
    case duckdb::TableReferenceType::BOUND_TABLE_REF:
      error = "table functions and system objects cannot be read";

      return;

    default:
      /* Joins, subqueries, VALUES, CTE references, empty FROM. */
      return;
  }
}

/** Only read-only query nodes may occur in CTEs and set operations. DuckDB's
expression iterator visits their contents without a node callback, so checking
only the outer SELECT statement misses COPY and DML CTEs.
@param[in]      node   parsed query node
@param[in,out]  error  first rejection reason; an existing reason is preserved
*/
static void query_check_read_node(duckdb::QueryNode &node, std::string &error) {
  if (!error.empty()) {
    return;
  }

  switch (node.type) {
    case duckdb::QueryNodeType::SELECT_NODE:
      break;

    case duckdb::QueryNodeType::SET_OPERATION_NODE: {
      duckdb::SetOperationNode &setop = node.Cast<duckdb::SetOperationNode>();
      for (duckdb::unique_ptr<duckdb::QueryNode> &child : setop.children) {
        query_check_read_node(*child, error);
      }
      break;
    }

    case duckdb::QueryNodeType::RECURSIVE_CTE_NODE: {
      duckdb::RecursiveCTENode &cte = node.Cast<duckdb::RecursiveCTENode>();
      query_check_read_node(*cte.left, error);
      query_check_read_node(*cte.right, error);
      break;
    }

    default:
      error = "only read-only SELECT queries are allowed, including in CTEs";

      return;
  }

  for (duckdb::pair<duckdb::Identifier,
                    duckdb::unique_ptr<duckdb::CommonTableExpressionInfo>>
           &cte : node.cte_map.map) {
    if (cte.second->query_node) {
      query_check_read_node(*cte.second->query_node, error);
    }
  }
}

/** Visit the PIVOT/UNPIVOT children omitted by DuckDB's table-ref iterator.
The pinned iterator visits the source and aggregates, but not the FOR
expressions or UNPIVOT entries. Both validation and range collection must
visit those children before a table reference can be treated as checked.
@param[in]  ref         table reference whose extra children are visited
@param[in]  visit_expr  visitor for each omitted expression
@param[in]  visit_node  visitor for a pivot-values subquery, if present */
static void query_pivot_children(
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

/** Check every table reference and query node, subqueries included.
@param[in]      node   parsed query node
@param[in,out]  error  first rejection reason; an existing reason is preserved
*/
static void query_check_node(duckdb::QueryNode &node, std::string &error) {
  query_check_read_node(node, error);
  if (!error.empty()) {
    return;
  }

  std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)> expr;

  expr = [&](duckdb::unique_ptr<duckdb::ParsedExpression> &child) {
    if (!error.empty()) {
      return;
    }

    if (child->GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      duckdb::SubqueryExpression &sub =
          child->Cast<duckdb::SubqueryExpression>();
      query_check_node(*sub.Subquery()->node, error);
    }

    duckdb::ParsedExpressionIterator::EnumerateChildren(*child, expr);
  };

  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
      node, expr, [&](duckdb::TableRef &ref) {
        query_pivot_children(ref, expr, [&](duckdb::QueryNode &child) {
          query_check_node(child, error);
        });

        /* The iterator already visits this subquery's expressions and
        table refs; check its query node types without walking them again. */
        if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
          duckdb::SubqueryRef &sub = ref.Cast<duckdb::SubqueryRef>();
          query_check_read_node(*sub.subquery->node, error);
        }
        query_check_ref(ref, error);
      });
}

/** Parse a statement and check what it reads.
@param[in]   sql    a single statement that DuckDB's C API parsed
@param[out]  error  reason for rejection
@return DB_SUCCESS, or DB_INVALID_ARGUMENT */
dberr_t query_check_statement(const std::string &sql, std::string &error) {
  duckdb::Parser parser = duckdb::Parser::GetBuiltinParser();
  parser.ParseQuery(sql);

  if (parser.statements.size() != 1) {
    error = "exactly one statement is required";

    return (DB_INVALID_ARGUMENT);
  }

  if (parser.statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
    error = "only SELECT statements are allowed";

    return (DB_INVALID_ARGUMENT);
  }

  duckdb::SelectStatement &select =
      parser.statements[0]->Cast<duckdb::SelectStatement>();
  query_check_node(*select.node, error);

  return (error.empty() ? DB_SUCCESS : DB_INVALID_ARGUMENT);
}

/** What the time conditions of a statement say about its table references. */
struct query_walk_t {
  /** Every table name reference. */
  std::vector<const duckdb::BaseTableRef *> refs;

  /** Ranges of the references that are the whole FROM of a SELECT. */
  std::map<const duckdb::TableRef *, query_range_t> bounded;
};

/** Check whether an expression names this table's time column.
@param[in]  expr   expression text or parsed expression to inspect
@param[in]  name   table name used when there is no alias
@param[in]  alias  table alias; empty when the table has no alias
@return true if an expression is the time column of the table reference with
  this name and alias */
static bool query_is_time(const duckdb::ParsedExpression &expr,
                          const std::string &name, const std::string &alias) {
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
@param[in]   expr  expression
@param[out]  us    the time in microseconds
@return false if the expression is not such a constant */
static bool query_time_constant(const duckdb::ParsedExpression &expr,
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
@param[in]      op     comparison operator
@param[in]      c      constant timestamp in microseconds */
static void query_range_narrow(query_range_t &range, duckdb::ExpressionType op,
                               int64_t c) {
  switch (op) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      range.lo = std::max(range.lo, c);
      range.hi = std::min(range.hi, c);

      return;

    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      if (c == INT64_MAX) {
        range.lo = INT64_MAX;
        range.hi = INT64_MIN;

        return;
      }
      range.lo = std::max(range.lo, c + 1);

      return;

    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      range.lo = std::max(range.lo, c);

      return;

    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      if (c == INT64_MIN) {
        range.lo = INT64_MAX;
        range.hi = INT64_MIN;

        return;
      }
      range.hi = std::min(range.hi, c - 1);

      return;

    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      range.hi = std::min(range.hi, c);

      return;

    default:
      return;
  }
}

/** Reverse the sides of a comparison operator.
@param[in]  op  comparison operator
@return a comparison with its sides swapped: c < time is time > c */
static duckdb::ExpressionType query_flip(duckdb::ExpressionType op) {
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
@param[in]      expr   expression text or parsed expression to inspect
@param[in]      name   table name used to resolve the time column
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

      if (query_is_time(cmp.Left(), name, alias) &&
          query_time_constant(cmp.Right(), c)) {
        query_range_narrow(range, op, c);
      } else if (query_is_time(cmp.Right(), name, alias) &&
                 query_time_constant(cmp.Left(), c)) {
        query_range_narrow(range, query_flip(op), c);
      }

      return;
    }

    case duckdb::ExpressionClass::BETWEEN: {
      const duckdb::BetweenExpression &between =
          expr.Cast<duckdb::BetweenExpression>();

      if (query_is_time(between.Input(), name, alias) &&
          query_time_constant(between.LowerBound(), c) &&
          query_time_constant(between.UpperBound(), c2)) {
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
inside table references and expressions are reached by the caller's walk.
@param[in]      node  parsed query node
@param[in,out]  walk  bounded table references collected from SELECT nodes and
                      CTEs */
static void query_ranges_selects(duckdb::QueryNode &node, query_walk_t &walk) {
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

        walk.bounded[from] = range;
      }
      break;
    }

    case duckdb::QueryNodeType::SET_OPERATION_NODE: {
      duckdb::SetOperationNode &setop = node.Cast<duckdb::SetOperationNode>();

      for (size_t i = 0; i < setop.children.size(); ++i) {
        query_ranges_selects(*setop.children[i], walk);
      }
      break;
    }

    case duckdb::QueryNodeType::RECURSIVE_CTE_NODE: {
      duckdb::RecursiveCTENode &cte = node.Cast<duckdb::RecursiveCTENode>();

      query_ranges_selects(*cte.left, walk);
      query_ranges_selects(*cte.right, walk);
      break;
    }

    default:
      break;
  }

  for (duckdb::pair<duckdb::Identifier,
                    duckdb::unique_ptr<duckdb::CommonTableExpressionInfo>> &kv :
       node.cte_map.map) {
    if (kv.second->query_node) {
      query_ranges_selects(*kv.second->query_node, walk);
    }
  }
}

/** Walk a query node, subqueries included, recording every table reference
and the ranges of the SELECTs.
@param[in]      node  parsed query node
@param[in,out]  walk  all table references and their SELECT time bounds */
static void query_ranges_node(duckdb::QueryNode &node, query_walk_t &walk) {
  query_ranges_selects(node, walk);

  std::function<void(duckdb::unique_ptr<duckdb::ParsedExpression> &)> expr;

  expr = [&](duckdb::unique_ptr<duckdb::ParsedExpression> &child) {
    if (child->GetExpressionClass() == duckdb::ExpressionClass::SUBQUERY) {
      duckdb::SubqueryExpression &sub =
          child->Cast<duckdb::SubqueryExpression>();
      query_ranges_node(*sub.Subquery()->node, walk);
    }

    duckdb::ParsedExpressionIterator::EnumerateChildren(*child, expr);
  };

  duckdb::ParsedExpressionIterator::EnumerateQueryNodeChildren(
      node, expr, [&](duckdb::TableRef &ref) {
        query_pivot_children(ref, expr, [&](duckdb::QueryNode &child) {
          query_ranges_node(child, walk);
        });

        if (ref.type == duckdb::TableReferenceType::BASE_TABLE) {
          walk.refs.push_back(&ref.Cast<duckdb::BaseTableRef>());
        } else if (ref.type == duckdb::TableReferenceType::SUBQUERY) {
          query_ranges_selects(*ref.Cast<duckdb::SubqueryRef>().subquery->node,
                               walk);
        }
      });
}

/** Work out the times a statement may read from each table, from simple
conditions on "time".
@param[in]   sql     statement accepted by query_check()
@param[out]  ranges  ranges found; empty if the statement cannot be read */
void query_find_ranges(const std::string &sql, query_ranges_t &ranges) {
  ranges.clear();

  duckdb::Parser parser = duckdb::Parser::GetBuiltinParser();
  parser.ParseQuery(sql);

  if (parser.statements.size() != 1 ||
      parser.statements[0]->type != duckdb::StatementType::SELECT_STATEMENT) {
    return;
  }

  query_walk_t walk;
  query_ranges_node(*parser.statements[0]->Cast<duckdb::SelectStatement>().node,
                    walk);

  /* A table gets the smallest range covering every reference to its name;
  one reference without a range leaves the table unbounded. */
  std::set<std::string> unbounded;
  query_ranges_t found;

  for (size_t i = 0; i < walk.refs.size(); ++i) {
    const duckdb::BaseTableRef *ref = walk.refs[i];
    const std::string &raw = ref->GetQualifiedName().Name().GetIdentifierName();
    std::string name;

    if (schema_table_name(raw.data(), raw.size(), name) != DB_SUCCESS) {
      continue;
    }

    std::map<const duckdb::TableRef *, query_range_t>::const_iterator b =
        walk.bounded.find(ref);

    if (b == walk.bounded.end()) {
      unbounded.insert(name);
      continue;
    }

    const query_range_t &range = b->second;

    query_ranges_t::iterator it = found.find(name);
    if (it == found.end()) {
      found[name] = range;
    } else if (range.lo <= range.hi) {
      /* An empty range adds nothing. */
      if (it->second.lo > it->second.hi) {
        it->second = range;
      } else {
        it->second.lo = std::min(it->second.lo, range.lo);
        it->second.hi = std::max(it->second.hi, range.hi);
      }
    }
  }

  for (query_ranges_t::iterator it = found.begin(); it != found.end(); ++it) {
    const bool all = it->second.lo == INT64_MIN && it->second.hi == INT64_MAX;

    if (!all && unbounded.count(it->first) == 0) {
      ranges[it->first] = it->second;
    }
  }
}
