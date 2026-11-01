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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_PARQUET_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_PARQUET_H_

#include "sql/engine/basic/ob_file_sql_reader.h"

namespace oceanbase
{
namespace sql
{

// Description of one leaf column of a parquet file, as found in the footer.
struct ObFileSqlParquetColumn
{
  ObFileSqlParquetColumn()
    : name_(), col_type_(FILE_SQL_COL_VARCHAR), physical_type_(0), nullable_(true) {}
  common::ObString name_;
  int64_t col_type_;      // the inferred logical type
  int32_t physical_type_; // parquet Type: 0 BOOLEAN, 1 INT32, 2 INT64, 4 FLOAT, 5 DOUBLE, 6 BYTE_ARRAY
  bool nullable_;
};

struct ObFileSqlParquetMeta
{
  ObFileSqlParquetMeta() : columns_(), row_cnt_(0) {}
  common::ObSEArray<ObFileSqlParquetColumn, 16, common::ModulePageAllocator, true> columns_;
  int64_t row_cnt_;
};

// Reads only the footer of the file: schema and row count.
int read_parquet_meta(common::ObIAllocator &alloc,
                      const common::ObString &path,
                      ObFileSqlParquetMeta &meta);

// Streaming parquet scanner.  Only the column chunks that are actually needed
// are decoded, one page at a time, so the memory used by a scan is bounded by
// the requested row count and not by the file size.
class ObFileSqlParquetReader : public ObFileSqlRowReader
{
public:
  ObFileSqlParquetReader();
  virtual ~ObFileSqlParquetReader() { close(); }
  int init(const ObFileSqlTableDef &def, common::ObIAllocator &alloc);
  virtual int open() override;
  virtual int get_next_row(common::ObIAllocator &alloc,
                           common::ObIArray<ObFileSqlCell> &row) override;
  virtual void close() override;
private:
  int load_meta();
  ObFileReader file_;
  bool file_opened_;
  bool inited_;
  common::ObString path_;
  int64_t column_count_;
  int64_t row_cnt_;
  int64_t row_idx_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_PARQUET_H_
