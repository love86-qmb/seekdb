/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/parser/ob_parser.h"
#include "sql/engine/basic/ob_file_sql_format.h"
#include "lib/string/ob_sql_string.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;

// The DuckDB shape of DESCRIBE: one row per output column of the inner query.
const static int64_t DESCRIBE_COLUMN_CNT = 6;
static const char *const DESCRIBE_COLUMN_NAMES[DESCRIBE_COLUMN_CNT] = {
  "column_name", "column_type", "null", "key", "default", "extra"
};

// Append a single quoted SQL string literal, doubling embedded quotes.
static int append_sql_literal(common::ObSqlString &sql, const common::ObString &text)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("'", 1))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < text.length(); ++i) {
    const char c = text.ptr()[i];
    if ('\'' == c) {
      if (OB_FAIL(sql.append("''", 2))) {
      }
    } else if (OB_FAIL(sql.append(&c, 1))) {
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append("'", 1))) {
  }
  return ret;
}

// Collect one (name, type name, nullable) triple per described column. A query
// reading a single file table is described from the schema the resolver inferred
// for that file, which is the only source of the DuckDB type names and of the
// nullability. Any other query is described from its select list.
static int collect_describe_columns(const ObSelectStmt &select_stmt,
                                    ObIArray<ObString> &names,
                                    ObIArray<ObString> &type_names,
                                    ObIArray<ObString> &nullable)
{
  int ret = OB_SUCCESS;
  const ObFileSqlTableDef *file_def = NULL;
  int64_t file_table_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_table_size(); ++i) {
    const TableItem *item = select_stmt.get_table_item(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null table item", K(ret));
    } else if (item->is_file_sql_table()) {
      file_def = item->file_sql_table_def_;
      ++file_table_cnt;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (1 == file_table_cnt && OB_NOT_NULL(file_def)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_def->column_count(); ++i) {
      if (OB_FAIL(names.push_back(file_def->column_names_.at(i)))) {
      } else if (OB_FAIL(type_names.push_back(ObString::make_string(
                   file_sql_col_type_name(file_def->column_types_.at(i)))))) {
      } else if (OB_FAIL(nullable.push_back(ObString::make_string(
                   file_def->column_nullable_.at(i) ? "YES" : "NO")))) {
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_select_item_size(); ++i) {
      const SelectItem &item = select_stmt.get_select_item(i);
      if (OB_ISNULL(item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null select expr", K(ret));
      } else if (OB_FAIL(names.push_back(item.alias_name_.empty()
                                         ? item.expr_name_ : item.alias_name_))) {
      } else if (OB_FAIL(type_names.push_back(ObString::make_string(
                   ob_obj_type_str(item.expr_->get_result_type().get_type()))))) {
      } else if (OB_FAIL(nullable.push_back(ObString::make_string("YES")))) {
      }
    }
  }
  return ret;
}

// `describe <query>`: resolve the inner query first (a file table infers its
// schema there), then replace the statement with the describing SELECT.
int ObExplainResolver::resolve_describe_statement(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
  ObStmt *child_stmt = NULL;
  if (OB_ISNULL(child_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty parse tree for stmt", K(ret));
  } else if (T_SELECT != child_node->type_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid stmt type for DESCRIBE", K(ret), "type", child_node->type_);
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DESCRIBE of a statement that is not a select");
  } else {
    ObResolver resolver(params_);
    if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *child_node, child_stmt))) {
    } else if (OB_ISNULL(child_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to generate stmt for the described query", K(ret));
    } else if (OB_UNLIKELY(!child_stmt->is_select_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the described query is not a select", K(ret));
    } else if (OB_FAIL(resolve_describe(static_cast<const ObSelectStmt &>(*child_stmt)))) {
    }
  }
  return ret;
}

// The describing statement is a plain UNION ALL of constant rows instead of an
// explain plan. The rows are generated from the schema of the described query,
// so the answer stays general, and being an ordinary SELECT it needs none of
// the plan-text machinery.
int ObExplainResolver::resolve_describe(const ObSelectStmt &child_stmt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSEArray<ObString, 16> names;
  ObSEArray<ObString, 16> type_names;
  ObSEArray<ObString, 16> nullable;
  if (OB_FAIL(collect_describe_columns(child_stmt, names, type_names, nullable))) {
  } else if (OB_UNLIKELY(names.count() != type_names.count()
                         || names.count() != nullable.count()
                         || names.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nothing to describe", K(ret), K(names.count()), K(type_names.count()));
  }
  for (int64_t row = 0; OB_SUCC(ret) && row < names.count(); ++row) {
    // The first branch also names the six output columns (null/key/default are
    // reserved words, hence the backticks).
    if (OB_FAIL(sql.append(0 == row ? "select " : " union all select "))) {
    }
    const ObString *values[3] = { &names.at(row), &type_names.at(row), &nullable.at(row) };
    for (int64_t col = 0; OB_SUCC(ret) && col < DESCRIBE_COLUMN_CNT; ++col) {
      if (0 != col && OB_FAIL(sql.append(", "))) {
      } else if (col >= 3) {
        // key/default/extra are always NULL for a file schema.
        if (OB_FAIL(sql.append("NULL"))) {
        }
      } else if (OB_FAIL(append_sql_literal(sql, *values[col]))) {
      }
      if (OB_SUCC(ret) && 0 == row
          && OB_FAIL(sql.append_fmt(" as `%s`", DESCRIBE_COLUMN_NAMES[col]))) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObParser parser(*allocator_,
                    session_info_->get_sql_mode(),
                    session_info_->get_charsets4parser());
    ParseResult parse_result;
    ObResolver resolver(params_);
    ObStmt *describe_stmt = NULL;
    if (OB_FAIL(parser.parse(sql.string(), parse_result))) {
      LOG_WARN("failed to parse the generated describe query", K(ret), K(sql));
    } else if (OB_ISNULL(parse_result.result_tree_)
               || parse_result.result_tree_->num_child_ < 1
               || OB_ISNULL(parse_result.result_tree_->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for the generated describe query", K(ret), K(sql));
    } else if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                        *parse_result.result_tree_->children_[0],
                                        describe_stmt))) {
    } else if (OB_ISNULL(describe_stmt) || !describe_stmt->is_select_stmt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the generated describe query is not a select", K(ret));
    } else {
      // The statement the resolver hands back is the describing SELECT.
      set_basic_stmt(describe_stmt);
    }
  }
  return ret;
}

int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL != parse_tree.children_[EXPLAIN_FORMAT]
             && T_BOOL == parse_tree.children_[EXPLAIN_FORMAT]->type_) {
    // DESCRIBE marks itself with a T_BOOL node in the format slot.
    ret = resolve_describe_statement(parse_tree);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        // The DESCRIBE marker (a T_BOOL in the format slot) never reaches here,
        // it is answered by resolve_describe_statement().
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
  }

  return ret;
}
