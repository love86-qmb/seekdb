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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_TABLE_OP_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_file_sql_format.h"
#include "sql/engine/basic/ob_file_sql_reader.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace sql
{

class ObFileSqlRowReader;

// Leaf operator that turns a local file into a row source.  It is a normal
// child of the plan, so filters, joins, ordering and aggregates above it are
// planned by the regular optimizer.
class ObFileSqlTableSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObFileSqlTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      format_(FILE_SQL_INVALID),
      path_(),
      json_format_(),
      header_(true),
      delimiter_(','),
      quote_('"'),
      column_cnt_(0),
      column_exprs_(alloc),
      column_index_(alloc),
      column_names_(alloc) {}
  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(format), K_(path), K_(json_format),
                       K_(header), K_(delimiter), K_(quote), K_(column_cnt));
  int64_t format_;
  common::ObString path_;
  common::ObString json_format_;
  bool header_;
  char delimiter_;
  char quote_;
  int64_t column_cnt_;
  // The columns of the file table that the statement actually uses, together
  // with their position in the file.  Everything else is pruned away, which is
  // why a `count(*)` may legitimately project no column at all.
  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_exprs_;
  common::ObFixedArray<int64_t, common::ObIAllocator> column_index_;
  // Names of all file columns, used by the format that is keyed by name.
  common::ObFixedArray<common::ObString, common::ObIAllocator> column_names_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFileSqlTableSpec);
};

class ObFileSqlTableOp : public ObOperator
{
public:
  ObFileSqlTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObFileSqlTableOp() { destroy_reader(); }
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override;
private:
  int create_reader();
  void destroy_reader();
  int convert_cell_to_datum(const ObFileSqlCell &cell, ObExpr *expr);
  common::ObArenaAllocator row_alloc_;
  ObFileSqlRowReader *reader_;
  common::ObSEArray<ObFileSqlCell, 32, common::ModulePageAllocator, true> cells_;
  bool reader_opened_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFileSqlTableOp);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_TABLE_OP_H_
