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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_file_sql_table_op.h"
#include "sql/engine/basic/ob_file_sql_reader.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

OB_SERIALIZE_MEMBER_INHERIT(ObFileSqlTableSpec, ObOpSpec,
                            format_, path_, json_format_, header_, delimiter_, quote_,
                            column_cnt_, column_exprs_, column_index_, column_names_);

ObFileSqlTableOp::ObFileSqlTableOp(ObExecContext &exec_ctx,
                                   const ObOpSpec &spec,
                                   ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    row_alloc_("FileSqlRow"),
    reader_(NULL),
    cells_(),
    reader_opened_(false)
{
}

int ObFileSqlTableOp::create_reader()
{
  int ret = OB_SUCCESS;
  ObFileSqlTableDef def;
  ObIAllocator &plan_alloc = ctx_.get_allocator();
  // The reader only needs the definition for the duration of one scan, so it is
  // rebuilt from the spec on open/rescan instead of being serialized.
  def.format_ = MY_SPEC.format_;
  def.path_ = MY_SPEC.path_;
  def.json_format_ = MY_SPEC.json_format_;
  def.header_ = MY_SPEC.header_;
  def.delimiter_ = MY_SPEC.delimiter_;
  def.quote_ = MY_SPEC.quote_;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_cnt_; ++i) {
    if (OB_FAIL(def.column_types_.push_back(FILE_SQL_COL_VARCHAR))) {
    } else if (OB_FAIL(def.column_nullable_.push_back(true))) {
    } else if (i < MY_SPEC.column_names_.count() &&
               OB_FAIL(def.column_names_.push_back(MY_SPEC.column_names_.at(i)))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_file_sql_reader(def, plan_alloc, reader_))) {
  } else if (OB_FAIL(reader_->open())) {
  } else {
    reader_opened_ = true;
  }
  return ret;
}

void ObFileSqlTableOp::destroy_reader()
{
  if (NULL != reader_) {
    reader_->close();
    reader_ = NULL;
  }
  reader_opened_ = false;
}

int ObFileSqlTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  // The switch has to be checked again at execution time: the schema was
  // inferred when the statement was resolved, and a plan cached by an earlier
  // session must not bypass the gate.
  if (OB_FAIL(check_file_sql_enabled(GET_MY_SESSION(ctx_)))) {
  } else if (OB_FAIL(create_reader())) {
    LOG_WARN("failed to open the file source", K(ret), K(MY_SPEC.path_));
  }
  return ret;
}

int ObFileSqlTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    // A rescan restarts the file from the beginning; the readers keep no state
    // across scans other than the file handle.
    destroy_reader();
    if (OB_FAIL(create_reader())) {
    }
  }
  return ret;
}

int ObFileSqlTableOp::inner_close()
{
  destroy_reader();
  row_alloc_.reset();
  return OB_SUCCESS;
}

void ObFileSqlTableOp::destroy()
{
  destroy_reader();
  row_alloc_.reset();
  ObOperator::destroy();
}

int ObFileSqlTableOp::convert_cell_to_datum(const ObFileSqlCell &cell, ObExpr *expr)
{
  int ret = OB_SUCCESS;
  ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
  if (cell.is_null_) {
    datum.set_null();
  } else {
    switch (expr->datum_meta_.type_) {
      case ObIntType: {
        int64_t value = 0;
        if (!file_sql_parse_int(cell.str_, value)) {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("file sql: value is not an integer", K(ret), K(cell.str_));
        } else {
          datum.set_int(value);
        }
        break;
      }
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObUTinyIntType: {
        int64_t value = 0;
        if (file_sql_parse_bool(cell.str_, value)) {
          datum.set_int(value);
        } else if (file_sql_parse_int(cell.str_, value)) {
          datum.set_int(value);
        } else {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("file sql: value is not a boolean", K(ret), K(cell.str_));
        }
        break;
      }
      case ObUInt64Type: {
        int64_t value = 0;
        if (!file_sql_parse_int(cell.str_, value)) {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("file sql: value is not an integer", K(ret), K(cell.str_));
        } else {
          datum.set_uint(static_cast<uint64_t>(value));
        }
        break;
      }
      case ObFloatType: {
        double value = 0;
        int64_t int_value = 0;
        if (file_sql_parse_double(cell.str_, value)) {
          datum.set_float(static_cast<float>(value));
        } else if (file_sql_parse_int(cell.str_, int_value)) {
          datum.set_float(static_cast<float>(int_value));
        } else {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("file sql: value is not a number", K(ret), K(cell.str_));
        }
        break;
      }
      case ObDoubleType: {
        double value = 0;
        int64_t int_value = 0;
        if (file_sql_parse_double(cell.str_, value)) {
          datum.set_double(value);
        } else if (file_sql_parse_int(cell.str_, int_value)) {
          datum.set_double(static_cast<double>(int_value));
        } else {
          ret = OB_INVALID_NUMERIC;
          LOG_WARN("file sql: value is not a number", K(ret), K(cell.str_));
        }
        break;
      }
      case ObDateType: {
        int32_t value = 0;
        if (!file_sql_parse_date(cell.str_, value)) {
          ret = OB_INVALID_DATE_VALUE;
          LOG_WARN("file sql: value is not a date", K(ret), K(cell.str_));
        } else {
          datum.set_date(value);
        }
        break;
      }
      case ObDateTimeType: {
        int64_t value = 0;
        if (!file_sql_parse_datetime(cell.str_, value)) {
          ret = OB_INVALID_DATE_VALUE;
          LOG_WARN("file sql: value is not a datetime", K(ret), K(cell.str_));
        } else {
          datum.set_datetime(value);
        }
        break;
      }
      default: {
        datum.set_string(cell.str_);
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    expr->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

int ObFileSqlTableOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(try_check_status())) {
  } else {
    clear_evaluated_flag();
    // The row buffer is only valid until the next row is fetched, which is the
    // usual contract of a row source in this engine.
    row_alloc_.reuse();
    cells_.reset();
    if (OB_FAIL(reader_->get_next_row(row_alloc_, cells_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to read next row of file", K(ret));
      }
    } else if (OB_UNLIKELY(cells_.count() != MY_SPEC.column_cnt_) ||
               OB_UNLIKELY(MY_SPEC.column_exprs_.count() != MY_SPEC.column_index_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file sql: column count mismatch", K(ret), K(cells_.count()),
               K(MY_SPEC.column_cnt_), K(MY_SPEC.column_exprs_.count()));
    } else {
      // Projected column i holds file column column_index_[i].
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
        const int64_t file_idx = MY_SPEC.column_index_.at(i);
        if (OB_UNLIKELY(file_idx < 0 || file_idx >= cells_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("file sql: bad column index", K(ret), K(file_idx));
        } else if (OB_FAIL(convert_cell_to_datum(cells_.at(file_idx),
                                                 MY_SPEC.column_exprs_.at(i)))) {
        }
      }
      // Anything the parent asked for that is not one of the file columns is a
      // function of them, so evaluate it from the datums just produced.
      for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); ++i) {
        ObExpr *output = MY_SPEC.output_.at(i);
        if (i < MY_SPEC.column_exprs_.count() && output == MY_SPEC.column_exprs_.at(i)) {
          /* already produced */
        } else {
          ObDatum *datum = NULL;
          if (OB_FAIL(output->eval(eval_ctx_, datum))) {
            LOG_WARN("failed to evaluate output expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
