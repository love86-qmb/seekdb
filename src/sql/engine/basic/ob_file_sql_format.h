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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_FORMAT_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_FORMAT_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
namespace sql
{

// Which table function produced a file source.  Kept as a plain integer so it
// can be serialized inside the physical operator spec.
enum ObFileSqlFormat
{
  FILE_SQL_INVALID = 0,
  FILE_SQL_CSV = 1,
  FILE_SQL_JSONL = 2,
  FILE_SQL_PARQUET = 3,
  FILE_SQL_GLOB = 4,
};

// The logical column types that type inference can produce for a file source.
// They are deliberately DuckDB-shaped: the DESCRIBE output prints them with
// `file_sql_col_type_name`, and they decide which OB object type the column is
// exposed as.
enum ObFileSqlColType
{
  FILE_SQL_COL_NULL = 0,      // every sampled value was NULL/absent
  FILE_SQL_COL_BOOLEAN = 1,
  FILE_SQL_COL_BIGINT = 2,
  FILE_SQL_COL_DOUBLE = 3,
  FILE_SQL_COL_VARCHAR = 4,
  FILE_SQL_COL_DATE = 5,
  FILE_SQL_COL_DATETIME = 6,
};

// The externally visible name of an inferred column type ("BIGINT", "DATE", ...).
const char *file_sql_col_type_name(int64_t col_type);

// The OB type an inferred column type is exposed as.  A column that only ever
// held NULL values is exposed as VARCHAR, which is what DuckDB does for its
// "NULL" type when the column has to materialise.
common::ObObjType file_sql_col_type_to_obj_type(int64_t col_type);

// Definition of one file table source.  It is produced by the resolver (which
// parses the arguments and runs type inference) and consumed by the optimizer
// (for the row count estimate) and by the executor (which streams the file).
struct ObFileSqlTableDef
{
  ObFileSqlTableDef()
    : format_(FILE_SQL_INVALID),
      path_(),
      json_format_(),
      header_(true),
      sample_size_(-1),
      delimiter_(','),
      quote_('"'),
      escape_('"'),
      column_names_(),
      column_types_(),
      column_nullable_(),
      projection_(),
      row_cnt_(0) {}

  int64_t format_;
  // Absolute path of the file, or the glob pattern for glob().
  common::ObString path_;
  // For read_json: the value of the `format` option.
  common::ObString json_format_;
  bool header_;
  int64_t sample_size_;
  char delimiter_;
  char quote_;
  char escape_;
  common::ObSEArray<common::ObString, 16, common::ModulePageAllocator, true> column_names_;
  common::ObSEArray<int64_t, 16, common::ModulePageAllocator, true> column_types_;
  // true when at least one sampled value of the column was NULL/empty.  Drives
  // the `null` column of DESCRIBE.
  common::ObSEArray<bool, 16, common::ModulePageAllocator, true> column_nullable_;
  // File columns the statement projects, so a reader can skip the others.  An
  // empty list means "no pruning information, read everything".
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> projection_;
  // Number of rows seen during inference; only used as a cardinality hint.
  int64_t row_cnt_;

  int64_t column_count() const { return column_types_.count(); }
  int assign(const ObFileSqlTableDef &other, common::ObIAllocator &alloc);
  static int deep_copy_string(common::ObIAllocator &alloc,
                              const common::ObString &src,
                              common::ObString &dst);
  TO_STRING_KV(K_(format), K_(path), K_(json_format), K_(header), K_(sample_size),
               K_(delimiter), K_(quote), K_(escape), K_(column_names),
               K_(column_types), K_(column_nullable), K_(row_cnt));
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_FORMAT_H_
