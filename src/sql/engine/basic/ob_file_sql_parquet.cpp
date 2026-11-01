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

#include "sql/engine/basic/ob_file_sql_parquet.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

int infer_parquet_file_sql_schema(ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObFileSqlParquetMeta meta;
  if (OB_FAIL(read_parquet_meta(alloc, def.path_, meta))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta.columns_.count(); ++i) {
      ObString name;
      if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc,
                                                     meta.columns_.at(i).name_, name))) {
      } else if (OB_FAIL(def.column_names_.push_back(name))) {
      } else if (OB_FAIL(def.column_types_.push_back(meta.columns_.at(i).col_type_))) {
      } else if (OB_FAIL(def.column_nullable_.push_back(meta.columns_.at(i).nullable_))) {
      }
    }
    if (OB_SUCC(ret)) {
      def.row_cnt_ = meta.row_cnt_;
    }
  }
  return ret;
}

int read_parquet_meta(ObIAllocator &alloc,
                      const ObString &path,
                      ObFileSqlParquetMeta &meta)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  UNUSED(path);
  UNUSED(meta);
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet");
  return ret;
}

int create_parquet_file_sql_reader(const ObFileSqlTableDef &def,
                                   ObIAllocator &alloc,
                                   ObFileSqlRowReader *&reader)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  reader = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ObFileSqlParquetReader)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObFileSqlParquetReader *parquet_reader = new (buf) ObFileSqlParquetReader();
    if (OB_FAIL(parquet_reader->init(def, alloc))) {
    } else {
      reader = parquet_reader;
    }
  }
  return ret;
}

ObFileSqlParquetReader::ObFileSqlParquetReader()
  : file_opened_(false), inited_(false), path_(), column_count_(0),
    row_cnt_(0), row_idx_(0)
{
}

int ObFileSqlParquetReader::init(const ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  path_ = def.path_;
  column_count_ = def.column_count();
  inited_ = true;
  return ret;
}

int ObFileSqlParquetReader::open()
{
  return file_.open(path_, false);
}

void ObFileSqlParquetReader::close()
{
  if (file_opened_) {
    file_.close();
    file_opened_ = false;
  }
  row_idx_ = 0;
}

int ObFileSqlParquetReader::get_next_row(ObIAllocator &alloc, ObIArray<ObFileSqlCell> &row)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  UNUSED(row);
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "read_parquet");
  return ret;
}

} // namespace sql
} // namespace oceanbase
