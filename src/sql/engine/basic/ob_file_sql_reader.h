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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_READER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_READER_H_

#include "sql/engine/basic/ob_file_sql_format.h"
#include "lib/file/ob_file.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace sql
{

class ObBasicSessionInfo;

// One cell of a row produced by a file scan.  The value is always carried as
// text so that the readers stay independent from the SQL datum encoding; the
// physical operator converts it into the datum of the target column type.
struct ObFileSqlCell
{
  ObFileSqlCell() : str_(), is_null_(true) {}
  common::ObString str_;
  bool is_null_;
};

// Base class of all file scanners.  Implementations are streaming: they never
// hold more than one row (plus a bounded IO buffer) at a time, so files of any
// size can be scanned and a scan can be restarted cheaply.
class ObFileSqlRowReader
{
public:
  ObFileSqlRowReader() : column_count_(0) {}
  virtual ~ObFileSqlRowReader() {}
  // Prepares the underlying file.  Must be called once before get_next_row.
  virtual int open() = 0;
  // Produces exactly `column_count` cells in `row`; text is allocated from
  // `alloc`, which the caller owns and resets between rows.  Returns
  // OB_ITER_END at the end of the file.
  virtual int get_next_row(common::ObIAllocator &alloc,
                           common::ObIArray<ObFileSqlCell> &row) = 0;
  virtual void close() = 0;
  TO_STRING_KV(K_(column_count));
protected:
  int64_t column_count_;
};

// A reader over a byte stream that is fetched in chunks.  Both the CSV and the
// JSONL reader work directly on this buffer, which is what makes the scan
// streaming instead of loading the whole file.
class ObFileSqlTextReader : public ObFileSqlRowReader
{
public:
  ObFileSqlTextReader();
  virtual ~ObFileSqlTextReader();
protected:
  int init_text_reader(const ObFileSqlTableDef &def);
  int open_text_reader();
  void close_text_reader();
  // Appends the next chunk of the file to the buffer, moving the unread part to
  // the front first so that the buffer only has to grow for very long records.
  int fill_more();
  // The bytes of the file that have been fetched but not consumed yet.
  common::ObString path_;   // shallow view of the definition's path, kept alive by the plan
  ObFileReader file_;
  bool file_opened_;
  char *buf_;
  int64_t buf_size_;
  int64_t data_len_;
  int64_t data_pos_;
  int64_t file_offset_;
  bool eof_;
};

class ObFileSqlCsvReader : public ObFileSqlTextReader
{
public:
  ObFileSqlCsvReader();
  virtual ~ObFileSqlCsvReader() { close(); }
  int init(const ObFileSqlTableDef &def, common::ObIAllocator &alloc);
  virtual int open() override;
  virtual int get_next_row(common::ObIAllocator &alloc,
                           common::ObIArray<ObFileSqlCell> &row) override;
  virtual void close() override;
private:
  // Splits the record that starts at data_pos_ into fields.  `complete` is false
  // when the record is cut by the end of the fetched data and the file is not
  // over yet, in which case the caller has to fetch more and retry.
  int parse_record(common::ObIAllocator &alloc,
                   common::ObIArray<ObFileSqlCell> &row,
                   bool &complete);
  bool header_pending_;
  char delimiter_;
  char quote_;
};

// A single value of a JSONL record.
class ObFileSqlJsonValue
{
public:
  enum Kind { J_NULL = 0, J_BOOL, J_INT, J_DOUBLE, J_STRING };
  ObFileSqlJsonValue() : kind_(J_NULL), int_value_(0), double_value_(0), str_() {}
  Kind kind_;
  int64_t int_value_;
  double double_value_;
  common::ObString str_;
};

struct ObFileSqlJsonField
{
  ObFileSqlJsonField() : key_(), value_() {}
  common::ObString key_;
  ObFileSqlJsonValue value_;
};

// Splits a newline-delimited JSON file into flat (key, value) pairs.  Shared by
// the reader (which maps them onto the inferred columns) and by schema
// inference (which builds the union of all keys).
class ObFileSqlJsonlParser : public ObFileSqlTextReader
{
public:
  ObFileSqlJsonlParser();
  virtual ~ObFileSqlJsonlParser() { close(); }
  int init(const ObFileSqlTableDef &def, common::ObIAllocator &alloc);
  int open();
  void close();
  // Returns OB_ITER_END at the end of the file.
  int next_record(common::ObIAllocator &alloc,
                  common::ObIArray<ObFileSqlJsonField> &fields);
  virtual int get_next_row(common::ObIAllocator &, common::ObIArray<ObFileSqlCell> &) override
  {
    return OB_NOT_SUPPORTED;
  }
private:
  int read_line(common::ObIAllocator &alloc, common::ObString &line, bool &has_line);
  int skip_ws(const char *&p, const char *end);
  int parse_string(common::ObIAllocator &alloc, const char *&p, const char *end,
                   common::ObString &str);
  int parse_value(common::ObIAllocator &alloc, const char *&p, const char *end,
                  ObFileSqlJsonValue &value);
};

class ObFileSqlJsonlReader : public ObFileSqlRowReader
{
public:
  ObFileSqlJsonlReader() : parser_(), keys_(), fields_() {}
  virtual ~ObFileSqlJsonlReader() { close(); }
  int init(const ObFileSqlTableDef &def, common::ObIAllocator &alloc);
  virtual int open() override;
  virtual int get_next_row(common::ObIAllocator &alloc,
                           common::ObIArray<ObFileSqlCell> &row) override;
  virtual void close() override;
private:
  int json_value_to_cell(common::ObIAllocator &alloc,
                         const ObFileSqlJsonValue &value,
                         ObFileSqlCell &cell);
  ObFileSqlJsonlParser parser_;
  ObSEArray<common::ObString, 16, common::ModulePageAllocator, true> keys_;
  ObSEArray<ObFileSqlJsonField, 16, common::ModulePageAllocator, true> fields_;
};

class ObFileSqlGlobReader : public ObFileSqlRowReader
{
public:
  ObFileSqlGlobReader() : files_(), pos_(0) {}
  virtual ~ObFileSqlGlobReader() {}
  int init(const ObFileSqlTableDef &def, common::ObIAllocator &alloc);
  virtual int open() override;
  virtual int get_next_row(common::ObIAllocator &alloc,
                           common::ObIArray<ObFileSqlCell> &row) override;
  virtual void close() override;
private:
  ObSEArray<common::ObString, 16, common::ModulePageAllocator, true> files_;
  int64_t pos_;
};

// ---- parquet (implemented in ob_file_sql_parquet.cpp) ----------------------
// Reads the footer metadata only; it is what schema inference needs.
int infer_parquet_file_sql_schema(ObFileSqlTableDef &def, common::ObIAllocator &alloc);
// Creates the streaming parquet scanner.
int create_parquet_file_sql_reader(const ObFileSqlTableDef &def,
                                   common::ObIAllocator &alloc,
                                   ObFileSqlRowReader *&reader);

// Creates a streaming reader for the given definition.  The def must already
// have its inferred column list filled in.
int create_file_sql_reader(const ObFileSqlTableDef &def,
                           common::ObIAllocator &alloc,
                           ObFileSqlRowReader *&reader);

// Runs type inference over the whole file and fills the column list (names,
// types and null-ability) plus the row-count hint of `def`.  This is why
// `sample_size = -1` is the only supported value: every record is inspected.
int infer_file_sql_schema(ObFileSqlTableDef &def, common::ObIAllocator &alloc);
int infer_csv_or_jsonl_schema(ObFileSqlTableDef &def, common::ObIAllocator &alloc);
int infer_jsonl_schema(ObFileSqlTableDef &def, common::ObIAllocator &alloc);

// Expands a glob pattern into the list of matching regular files.  Only `*`
// and `?` are supported as wildcards, which is what the SQL layer exposes.
int glob_file_sql_paths(common::ObIAllocator &alloc,
                        const common::ObString &pattern,
                        common::ObIArray<common::ObString> &paths);

// Returns OB_NOT_SUPPORTED unless the session variable enable_file_sql is on.
int check_file_sql_enabled(ObBasicSessionInfo *session_info);

// Validates that the current session may touch `path`: the session variable
// enable_file_sql must be on *and* the path must live inside secure_file_priv.
int check_file_sql_privilege(ObBasicSessionInfo *session_info,
                             const common::ObString &path);

// Canonicalises `path` (resolving symlinks and "..") and stores a copy in the
// allocator.
int resolve_file_sql_path(common::ObIAllocator &alloc,
                          const common::ObString &path,
                          common::ObString &resolved);

// ---- strict scalar parsers shared by inference and by the scan operator ----
// "Strict" means the whole string has to match; the same helpers both detect a
// column type and convert values once the type is known.
bool file_sql_parse_int(const common::ObString &str, int64_t &value);
bool file_sql_parse_double(const common::ObString &str, double &value);
bool file_sql_parse_bool(const common::ObString &str, int64_t &value);
bool file_sql_parse_date(const common::ObString &str, int32_t &value);
bool file_sql_parse_datetime(const common::ObString &str, int64_t &value);

// Detects the most specific logical type of a single text value.
int file_sql_detect_type(const common::ObString &str, int64_t &col_type);
// Merges two inferred types following DuckDB's widening rules.
int64_t file_sql_merge_type(int64_t lhs, int64_t rhs);

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_FILE_SQL_READER_H_
