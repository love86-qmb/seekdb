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

#ifndef _OB_LOG_FILE_SQL_TABLE_H
#define _OB_LOG_FILE_SQL_TABLE_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/engine/basic/ob_file_sql_format.h"

namespace oceanbase
{
namespace sql
{
class FileSqlTablePath;

// Logical counterpart of the file table scan.  It is a leaf operator that
// produces the columns of the file table function; everything above it is a
// completely ordinary subplan.
class ObLogFileSqlTable : public ObLogicalOperator
{
public:
  ObLogFileSqlTable(ObLogPlan &plan)
    : ObLogicalOperator(plan),
      table_def_(NULL),
      table_name_(),
      table_id_(common::OB_INVALID_ID),
      file_path_(NULL),
      column_exprs_() {}
  virtual ~ObLogFileSqlTable() {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost,
                             double &cost);
  virtual int compute_equal_set() override;
  virtual int compute_table_set() override;
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  virtual int allocate_expr_post(ObAllocExprContext &ctx) override;
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
  // A scan that projects nothing (count(*), for instance) still needs an output
  // expression, so a constant is injected.
  int allocate_dummy_output();

  const common::ObIArray<ObColumnRefRawExpr *> &get_column_exprs() const { return column_exprs_; }
  common::ObIArray<ObColumnRefRawExpr *> &get_column_exprs() { return column_exprs_; }
  const ObFileSqlTableDef *get_table_def() const { return table_def_; }
  void set_table_def(ObFileSqlTableDef *table_def) { table_def_ = table_def; }
  const common::ObString &get_table_name() const { return table_name_; }
  void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  uint64_t get_table_id() const { return table_id_; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  void set_file_path(FileSqlTablePath *file_path) { file_path_ = file_path; }
  const FileSqlTablePath *get_file_path() const { return file_path_; }
private:
  ObFileSqlTableDef *table_def_;
  common::ObString table_name_;
  uint64_t table_id_;
  FileSqlTablePath *file_path_;
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> column_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObLogFileSqlTable);
};

} // namespace sql
} // namespace oceanbase

#endif // _OB_LOG_FILE_SQL_TABLE_H
