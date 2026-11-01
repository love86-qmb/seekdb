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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_explain_log_plan.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/code_generator/ob_code_generator.h"
#include "sql/engine/basic/ob_file_sql_format.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::sql::log_op_def;

/**
 *  This function does two things:
 *
 *  1. generate a logical plan for the 'real' query that's being 'explained'
 *  2. generate the plan text from the logical plan and put the text in the buffer
 *     and remember the logical plan as well.
 */
int ObExplainLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_explain_stmt())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObLogPlan *child_plan = NULL;
    const ObDMLStmt *child_stmt = NULL;
    ObLogicalOperator *top = NULL;
    ObLogValues *values_op = NULL;
    int64_t batch_size = 0;
    const ObExplainStmt *explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
    if (OB_ISNULL(child_stmt = explain_stmt->get_explain_query_stmt())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(child_plan = optimizer_context_.get_log_plan_factory().
                         create(optimizer_context_, *child_stmt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to create log plan for explain stmt");
    } else if (OB_FAIL(child_plan->generate_plan())) {
    } else if (OB_FAIL(remove_duplicate_constraints())) {
    } else if (OB_FAIL(check_explain_generate_plan_with_outline(child_plan))) {
    } else if (OB_FAIL(ObCodeGenerator::detect_batch_size(*child_plan, batch_size))) {
    } else if (OB_FAIL(allocate_values_as_top(top))) {
    } else if (OB_ISNULL(top) || OB_UNLIKELY(LOG_VALUES != top->get_type())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (explain_stmt->is_describe()) {
      // DESCRIBE returns the column description of the inner statement, not its
      // plan, so the describing rows replace the plan text.
      set_plan_root(top);
      top->mark_is_plan_root();
      child_plan->get_optimizer_context().set_batch_size(batch_size);
      // Same as the plan-text path: the describe rows are built from ObObj
      // cells, so the datums must not be packed by memcpy.
      optimizer_context_.set_packed(false);
      // The tail of this function uses values_op, so it has to be set here as
      // well; values_op->is_describe() then tells the tail not to add the
      // single output expression it uses for the plan text.
      values_op = static_cast<ObLogValues *>(top);
      values_op->set_describe_plan(true);
      if (OB_FAIL(generate_describe_plan(*top, *child_stmt))) {
      } else {
        get_optimizer_context().set_plan_type(ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                             ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                             false);
      }
    } else {
      set_plan_root(top);
      top->mark_is_plan_root();
      child_plan->get_optimizer_context().set_batch_size(batch_size);
      values_op = static_cast<ObLogValues*>(top);
      values_op->set_explain_plan(child_plan);
      ObSqlPlan sql_plan(get_allocator());
      sql_plan.set_session_info(get_optimizer_context().get_session_info());
      ObExplainLogPlan *explain_plan = static_cast<ObExplainLogPlan*>(child_plan);
      ObSEArray<common::ObString, 64> plan_strs;
      const ObString& into_table = explain_stmt->get_into_table();
      const ObString& statement_id = explain_stmt->get_statement_id();
      if (OB_FAIL(sql_plan.store_sql_plan_for_explain(get_optimizer_context().get_exec_ctx(),
                                                      child_plan,
                                                      explain_stmt->get_explain_type(),
                                                      0 == into_table.length() ? "PLAN_TABLE" : into_table,
                                                      0 == statement_id.length() ? "" : statement_id,
                                                      explain_stmt->get_display_opt(),
                                                      plan_strs))) {
      } else {
        //For explain stmt, we can do pack at the stage of expr alloc,
        //But we need to use the FALSE flag to tell the driver 
        //to use normal encoding instead of memcopy
        optimizer_context_.set_packed(false);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < plan_strs.count(); ++i) {
        ObObj obj;
        obj.set_varchar(plan_strs.at(i));
        ObNewRow row;
        row.cells_ = &obj;
        row.count_ = 1;
        if (OB_FAIL(values_op->add_row(row))) {
        }
      }
      if (OB_SUCC(ret)) {
        // set values_op operator id && set max operator id for LogPlan
        values_op->set_op_id(0);
        set_max_op_id(1);
      }
    }
    get_optimizer_context().get_all_exprs().reuse();
    if (OB_FAIL(ret)) {
    } else if (values_op->is_describe_plan()) {
      // The describing path already created one output expression per column.
    } else if (OB_FAIL(allocate_output_expr_for_values_op(*values_op))) {
    } else {
      get_optimizer_context().set_plan_type(ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            ObPhyPlanType::OB_PHY_PLAN_LOCAL,
                                            false);
    }
  }
  return ret;
}

// DuckDB style DESCRIBE: one row per output column, with six columns.
int ObExplainLogPlan::generate_describe_plan(ObLogicalOperator &top,
                                            const ObDMLStmt &child_stmt)
{
  int ret = OB_SUCCESS;
  ObLogValues *values_op = NULL;
  const int64_t DESCRIBE_COLUMN_CNT = 6;
  static const char *const describe_column_names[DESCRIBE_COLUMN_CNT] = {
    "column_name", "column_type", "null", "key", "default", "extra"
  };
  if (OB_UNLIKELY(LOG_VALUES != top.get_type())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    values_op = static_cast<ObLogValues *>(&top);
    const ObFileSqlTableDef *file_def = NULL;
    // When the inner statement reads a single file table, the description comes
    // straight from the inferred schema; it is the only source that knows the
    // column types and which columns saw a NULL.
    if (child_stmt.is_select_stmt()) {
      const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(child_stmt);
      const TableItem *single_item = NULL;
      int64_t table_cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_table_size(); ++i) {
        const TableItem *item = select_stmt.get_table_item(i);
        if (OB_ISNULL(item)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (item->is_file_sql_table()) {
          single_item = item;
          ++table_cnt;
        }
      }
      if (OB_SUCC(ret) && 1 == table_cnt && OB_NOT_NULL(single_item)) {
        file_def = single_item->file_sql_table_def_;
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObSEArray<ObString, 8> names;
      ObSEArray<ObString, 8> type_names;
      ObSEArray<ObString, 8> nullable;
      if (OB_NOT_NULL(file_def)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < file_def->column_count(); ++i) {
          if (OB_FAIL(names.push_back(file_def->column_names_.at(i)))) {
          } else if (OB_FAIL(type_names.push_back(ObString::make_string(
                       file_sql_col_type_name(file_def->column_types_.at(i)))))) {
          } else if (OB_FAIL(nullable.push_back(ObString::make_string(
                       file_def->column_nullable_.at(i) ? "YES" : "NO")))) {
          }
        }
      } else if (child_stmt.is_select_stmt()) {
        const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(child_stmt);
        for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_select_item_size(); ++i) {
          const SelectItem &item = select_stmt.get_select_item(i);
          if (OB_ISNULL(item.expr_)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(names.push_back(item.alias_name_))) {
          } else if (OB_FAIL(type_names.push_back(ObString::make_string(
                       ob_obj_type_str(item.expr_->get_result_type().get_type()))))) {
          } else if (OB_FAIL(nullable.push_back(ObString::make_string("YES")))) {
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "describe of a non select statement");
      }
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < names.count(); ++row_idx) {
        ObObj cells[DESCRIBE_COLUMN_CNT];
        ObNewRow row;
        cells[0].set_varchar(names.at(row_idx));
        cells[1].set_varchar(type_names.at(row_idx));
        cells[2].set_varchar(nullable.at(row_idx));
        cells[3].set_null();
        cells[4].set_null();
        cells[5].set_null();
        for (int64_t c = 0; OB_SUCC(ret) && c < DESCRIBE_COLUMN_CNT; ++c) {
          UNUSED(describe_column_names[c]);
          cells[c].set_collation_type(ObCharset::get_system_collation());
        }
        row.cells_ = cells;
        row.count_ = DESCRIBE_COLUMN_CNT;
        if (OB_FAIL(values_op->add_row(row))) {
        }
      }
      if (OB_SUCC(ret)) {
        values_op->set_op_id(0);
        set_max_op_id(1);
        // One output expression per describe column.
        for (int64_t c = 0; OB_SUCC(ret) && c < DESCRIBE_COLUMN_CNT; ++c) {
          ObConstRawExpr *output = NULL;
          if (OB_FAIL(optimizer_context_.get_expr_factory().create_raw_expr(T_VARCHAR, output))) {
          } else {
            ObObj v;
            v.set_varchar(ObString::make_string(" "));
            v.set_collation_type(ObCharset::get_system_collation());
            output->set_value(v);
            if (OB_FAIL(output->formalize(optimizer_context_.get_session_info()))) {
            } else if (OB_FAIL(values_op->get_output_exprs().push_back(output))) {
            } else if (OB_FAIL(optimizer_context_.get_all_exprs().append(output))) {
            }
          }
        }
        if (OB_SUCC(ret)) {
          values_op->set_branch_id(0);
          values_op->set_id(0);
          values_op->set_op_id(0);
        }
      }
    }
  }
  return ret;
}

int ObExplainLogPlan::check_explain_generate_plan_with_outline(ObLogPlan *real_plan)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  ObSqlCtx *sql_ctx = NULL;
  ObSQLSessionInfo *session_info = NULL;
  const ObExplainStmt *explain_stmt = static_cast<const ObExplainStmt*>(get_stmt());
  ObQueryCtx *query_ctx = get_optimizer_context().get_query_ctx();
  if (OB_ISNULL(real_plan) || OB_ISNULL(explain_stmt)
      || OB_ISNULL(exec_ctx = get_optimizer_context().get_exec_ctx())
      || OB_ISNULL(session_info = get_optimizer_context().get_session_info())
      || OB_ISNULL(sql_ctx = exec_ctx->get_sql_ctx()) || OB_ISNULL(query_ctx)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (session_info->is_inner() || sql_ctx->is_prepare_protocol_) {
    /* do not check explain for inner sql (include query in PL) */
  } else if (OB_UNLIKELY(get_optimizer_context().get_query_ctx() != NULL
                         && get_optimizer_context().get_query_ctx()->get_injected_random_status())) { 
    /* do nothing */
  } else if (sql_ctx->multi_stmt_item_.is_part_of_multi_stmt()
             && sql_ctx->multi_stmt_item_.get_seq_num() > 0) {
    /* generate plan call by ObMPQuery::process_with_tmp_context use tmp context, do not check */
  } else if (EXPLAIN_UNINITIALIZED !=  explain_stmt->get_explain_type()
             && EXPLAIN_BASIC !=  explain_stmt->get_explain_type()
             && EXPLAIN_OUTLINE !=  explain_stmt->get_explain_type()) {
    /* generate plan again for explain/explain basic/explain outline,
      do not check explain extended/explain extended_noaddr */
  } else if (0 == sql_ctx->first_plan_hash_) {  /* generate plan first time */
    void *tmp_ptr = NULL;
    sql_ctx->first_outline_data_.reset();
    if (OB_UNLIKELY(0 == real_plan->get_signature()) || 0 < sql_ctx->retry_times_) {
      /* do nothing */
    } else if (OB_SUCC(OB_E(EventTable::EN_EXPLAIN_GENERATE_PLAN_WITH_OUTLINE) OB_SUCCESS)) {
      /* do nothing */
    } else if (OB_UNLIKELY(NULL == (tmp_ptr = get_optimizer_context().get_allocator().alloc(OB_MAX_SQL_LENGTH)))) {
      /* allocator in optimizer context is from ObResultSet */
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory", K(ret));
    } else {
      PlanText plan_text;
      plan_text.buf_ = static_cast<char *>(tmp_ptr);
      plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
      if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, real_plan))) {
      } else {
        sql_ctx->first_outline_data_.assign_ptr(plan_text.buf_, static_cast<ObString::obstr_size_t>(plan_text.pos_));
        sql_ctx->first_plan_hash_ = real_plan->get_signature();
        sql_ctx->first_equal_param_cons_cnt_ = query_ctx->all_equal_param_constraints_.count();
        sql_ctx->first_const_param_cons_cnt_ = query_ctx->all_plan_const_param_constraints_.count();
        sql_ctx->first_expr_cons_cnt_ = query_ctx->all_expr_constraints_.count();
        ret = OB_SQL_RETRY_OUTLINE;
      }
    }
  } else {  /* check generate plan again use outline data */
    SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
      PlanText plan_text;
      plan_text.buf_ = buf;
      plan_text.buf_len_ = OB_MAX_SQL_LENGTH;
      if (OB_FAIL(ObSqlPlan::get_plan_outline_info_one_line(plan_text, real_plan))) {
      } else {
        ObString cur_outline_data;
        const uint64_t cur_plan_hash = real_plan->get_signature();
        cur_outline_data.assign_ptr(plan_text.buf_, static_cast<ObString::obstr_size_t>(plan_text.pos_));
        if (cur_plan_hash != sql_ctx->first_plan_hash_) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_plan_hash_));
        }
        if (0 != (cur_outline_data.case_compare(sql_ctx->first_outline_data_))) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_outline_data_));
        }
        if (query_ctx->all_equal_param_constraints_.count() != sql_ctx->first_equal_param_cons_cnt_ || 
            query_ctx->all_plan_const_param_constraints_.count() != sql_ctx->first_const_param_cons_cnt_ || 
            query_ctx->all_expr_constraints_.count() != sql_ctx->first_expr_cons_cnt_) {
          ret = OB_OUTLINE_NOT_REPRODUCIBLE;
          LOG_WARN("failed to generate plan use outline", K(sql_ctx->first_equal_param_cons_cnt_), 
                                                          K(sql_ctx->first_const_param_cons_cnt_), 
                                                          K(sql_ctx->first_expr_cons_cnt_),
                                                          K(query_ctx->all_equal_param_constraints_.count()),
                                                          K(query_ctx->all_plan_const_param_constraints_.count()),
                                                          K(query_ctx->all_expr_constraints_.count()));
        }
        if (OB_FAIL(ret)) {
        }
      }
      sql_ctx->first_plan_hash_ = 0;
      sql_ctx->first_outline_data_.reset();
      sql_ctx->first_equal_param_cons_cnt_ = 0;
      sql_ctx->first_const_param_cons_cnt_ = 0;
      sql_ctx->first_expr_cons_cnt_ = 0;
    }
  }
  return ret;
}
