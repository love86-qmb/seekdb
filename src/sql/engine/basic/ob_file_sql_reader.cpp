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

#include "sql/engine/basic/ob_file_sql_reader.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_basic_session_info.h"
#include "share/system_variable/ob_sys_var_class_type.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{
// Size of the IO buffer used by the text readers.  It bounds the memory of a
// scan regardless of the file size.
const int64_t FILE_SQL_READER_BUF_SIZE = 1 << 20;  // 1MB

// Shell style wildcard match with '*' and '?'.  Used by glob() only.
bool match_wildcard(const char *pattern, const char *name)
{
  const char *star = NULL;
  const char *resume = NULL;
  while ('\0' != *name) {
    if ('*' == *pattern) {
      star = pattern++;
      resume = name;
    } else if ('?' == *pattern || *pattern == *name) {
      ++pattern;
      ++name;
    } else if (NULL != star) {
      pattern = star + 1;
      name = ++resume;
    } else {
      return false;
    }
  }
  while ('*' == *pattern) {
    ++pattern;
  }
  return '\0' == *pattern;
}
} // namespace

const char *file_sql_col_type_name(int64_t col_type)
{
  const char *name = "VARCHAR";
  switch (col_type) {
    case FILE_SQL_COL_BOOLEAN: name = "BOOLEAN"; break;
    case FILE_SQL_COL_BIGINT: name = "BIGINT"; break;
    case FILE_SQL_COL_DOUBLE: name = "DOUBLE"; break;
    case FILE_SQL_COL_DATE: name = "DATE"; break;
    case FILE_SQL_COL_DATETIME: name = "DATETIME"; break;
    default: name = "VARCHAR"; break;
  }
  return name;
}

ObObjType file_sql_col_type_to_obj_type(int64_t col_type)
{
  ObObjType type = ObVarcharType;
  switch (col_type) {
    case FILE_SQL_COL_BOOLEAN: type = ObTinyIntType; break;
    case FILE_SQL_COL_BIGINT: type = ObIntType; break;
    case FILE_SQL_COL_DOUBLE: type = ObDoubleType; break;
    case FILE_SQL_COL_DATE: type = ObDateType; break;
    case FILE_SQL_COL_DATETIME: type = ObDateTimeType; break;
    default: type = ObVarcharType; break;
  }
  return type;
}

int ObFileSqlTableDef::deep_copy_string(ObIAllocator &alloc,
                                        const ObString &src,
                                        ObString &dst)
{
  int ret = OB_SUCCESS;
  if (src.empty()) {
    dst.reset();
  } else {
    char *buf = static_cast<char *>(alloc.alloc(src.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, src.ptr(), src.length());
      dst.assign_ptr(buf, src.length());
    }
  }
  return ret;
}

int ObFileSqlTableDef::assign(const ObFileSqlTableDef &other, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  format_ = other.format_;
  header_ = other.header_;
  sample_size_ = other.sample_size_;
  delimiter_ = other.delimiter_;
  quote_ = other.quote_;
  escape_ = other.escape_;
  row_cnt_ = other.row_cnt_;
  if (OB_FAIL(deep_copy_string(alloc, other.path_, path_))) {
  } else if (OB_FAIL(deep_copy_string(alloc, other.json_format_, json_format_))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.column_names_.count(); ++i) {
    ObString name;
    if (OB_FAIL(deep_copy_string(alloc, other.column_names_.at(i), name))) {
    } else if (OB_FAIL(column_names_.push_back(name))) {
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.column_types_.count(); ++i) {
    if (OB_FAIL(column_types_.push_back(other.column_types_.at(i)))) {
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.column_nullable_.count(); ++i) {
    if (OB_FAIL(column_nullable_.push_back(other.column_nullable_.at(i)))) {
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// strict scalar parsing
// ---------------------------------------------------------------------------

bool file_sql_parse_int(const ObString &str, int64_t &value)
{
  bool ok = false;
  const char *p = str.ptr();
  const int64_t len = str.length();
  if (NULL != p && len > 0 && len < 32) {
    int64_t i = 0;
    bool negative = false;
    if ('-' == p[i] || '+' == p[i]) {
      negative = ('-' == p[i]);
      ++i;
    }
    int64_t acc = 0;
    int64_t digits = 0;
    bool overflow = false;
    for (; i < len && !overflow; ++i) {
      if (p[i] < '0' || p[i] > '9') {
        break;
      }
      const int64_t digit = p[i] - '0';
      if (acc > (INT64_MAX - digit) / 10) {
        overflow = true;
      } else {
        acc = acc * 10 + digit;
        ++digits;
      }
    }
    if (!overflow && i == len && digits > 0) {
      value = negative ? -acc : acc;
      ok = true;
    }
  }
  return ok;
}

bool file_sql_parse_double(const ObString &str, double &value)
{
  // strtod accepts leading blanks, "inf"/"nan" and trailing garbage, so the
  // shape is validated first and only then handed over to strtod.
  bool ok = false;
  const char *p = str.ptr();
  const int64_t len = str.length();
  if (NULL != p && len > 0 && len < 64) {
    int64_t i = 0;
    if ('-' == p[i] || '+' == p[i]) {
      ++i;
    }
    int64_t int_digits = 0;
    while (i < len && p[i] >= '0' && p[i] <= '9') {
      ++i;
      ++int_digits;
    }
    int64_t frac_digits = 0;
    if (i < len && '.' == p[i]) {
      ++i;
      while (i < len && p[i] >= '0' && p[i] <= '9') {
        ++i;
        ++frac_digits;
      }
    }
    bool has_exp = false;
    if (i < len && ('e' == p[i] || 'E' == p[i])) {
      ++i;
      if (i < len && ('-' == p[i] || '+' == p[i])) {
        ++i;
      }
      int64_t exp_digits = 0;
      while (i < len && p[i] >= '0' && p[i] <= '9') {
        ++i;
        ++exp_digits;
      }
      has_exp = (exp_digits > 0);
    }
    if (i == len && (int_digits > 0 || frac_digits > 0) &&
        (frac_digits > 0 || has_exp)) {
      char buf[80];
      MEMCPY(buf, p, len);
      buf[len] = '\0';
      char *end = NULL;
      const double parsed = strtod(buf, &end);
      if (NULL != end && '\0' == *end) {
        value = parsed;
        ok = true;
      }
    }
  }
  return ok;
}

bool file_sql_parse_bool(const ObString &str, int64_t &value)
{
  bool ok = true;
  if (4 == str.length() && 0 == str.case_compare(ObString::make_string("true"))) {
    value = 1;
  } else if (5 == str.length() && 0 == str.case_compare(ObString::make_string("false"))) {
    value = 0;
  } else {
    ok = false;
  }
  return ok;
}

// "YYYY-MM-DD" including a real calendar check.
bool file_sql_parse_date(const ObString &str, int32_t &value)
{
  bool ok = false;
  const char *p = str.ptr();
  if (NULL != p && 10 == str.length() && '-' == p[4] && '-' == p[7]) {
    bool digits_ok = true;
    for (int64_t i = 0; digits_ok && i < 10; ++i) {
      if (4 == i || 7 == i) {
        continue;
      }
      digits_ok = (p[i] >= '0' && p[i] <= '9');
    }
    const int64_t year = (p[0] - '0') * 1000 + (p[1] - '0') * 100 + (p[2] - '0') * 10 + (p[3] - '0');
    const int64_t month = (p[5] - '0') * 10 + (p[6] - '0');
    const int64_t day = (p[8] - '0') * 10 + (p[9] - '0');
    if (digits_ok && year >= 1 && year <= 9999 && month >= 1 && month <= 12 && day >= 1) {
      static const int32_t days_in_month[13] = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
      };
      int32_t max_day = days_in_month[month];
      const bool leap = (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
      if (2 == month && leap) {
        max_day = 29;
      }
      if (day <= max_day && OB_SUCCESS == ObTimeConverter::str_to_date(str, value)) {
        ok = true;
      }
    }
  }
  return ok;
}

// "YYYY-MM-DD HH:MM:SS" and the ISO variant that uses a 'T' separator.
bool file_sql_parse_datetime(const ObString &str, int64_t &value)
{
  bool ok = false;
  const char *p = str.ptr();
  const int64_t len = str.length();
  if (NULL != p && len >= 19 && len < 40 && ('T' == p[10] || ' ' == p[10]) &&
      ':' == p[13] && ':' == p[16]) {
    char buf[64];
    MEMCPY(buf, p, len);
    if ('T' == buf[10]) {
      buf[10] = ' ';
    }
    const ObString norm(static_cast<int32_t>(len), buf);
    const ObString date_part(10, buf);
    int32_t date_value = 0;
    if (file_sql_parse_date(date_part, date_value)) {
      ObTimeConvertCtx cvrt_ctx(NULL, false);
      int16_t scale = 0;
      if (OB_SUCCESS == ObTimeConverter::str_to_datetime(norm, cvrt_ctx, value, &scale)) {
        ok = true;
      }
    }
  }
  return ok;
}

int file_sql_detect_type(const ObString &str, int64_t &col_type)
{
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  double double_value = 0;
  int32_t date_value = 0;
  int64_t datetime_value = 0;
  if (str.empty()) {
    col_type = FILE_SQL_COL_NULL;
  } else if (file_sql_parse_int(str, int_value)) {
    col_type = FILE_SQL_COL_BIGINT;
  } else if (file_sql_parse_bool(str, int_value)) {
    col_type = FILE_SQL_COL_BOOLEAN;
  } else if (file_sql_parse_double(str, double_value)) {
    col_type = FILE_SQL_COL_DOUBLE;
  } else if (file_sql_parse_date(str, date_value)) {
    col_type = FILE_SQL_COL_DATE;
  } else if (file_sql_parse_datetime(str, datetime_value)) {
    col_type = FILE_SQL_COL_DATETIME;
  } else {
    col_type = FILE_SQL_COL_VARCHAR;
  }
  return ret;
}

int64_t file_sql_merge_type(int64_t lhs, int64_t rhs)
{
  int64_t merged = FILE_SQL_COL_VARCHAR;
  if (FILE_SQL_COL_NULL == lhs) {
    merged = rhs;
  } else if (FILE_SQL_COL_NULL == rhs) {
    merged = lhs;
  } else if (lhs == rhs) {
    merged = lhs;
  } else {
    const bool lhs_temporal = (FILE_SQL_COL_DATE == lhs || FILE_SQL_COL_DATETIME == lhs);
    const bool rhs_temporal = (FILE_SQL_COL_DATE == rhs || FILE_SQL_COL_DATETIME == rhs);
    const bool lhs_numeric = (FILE_SQL_COL_BOOLEAN == lhs || FILE_SQL_COL_BIGINT == lhs ||
                              FILE_SQL_COL_DOUBLE == lhs);
    const bool rhs_numeric = (FILE_SQL_COL_BOOLEAN == rhs || FILE_SQL_COL_BIGINT == rhs ||
                              FILE_SQL_COL_DOUBLE == rhs);
    if (lhs_temporal && rhs_temporal) {
      merged = FILE_SQL_COL_DATETIME;
    } else if (lhs_numeric && rhs_numeric) {
      if (FILE_SQL_COL_DOUBLE == lhs || FILE_SQL_COL_DOUBLE == rhs) {
        merged = FILE_SQL_COL_DOUBLE;
      } else if (FILE_SQL_COL_BIGINT == lhs || FILE_SQL_COL_BIGINT == rhs) {
        merged = FILE_SQL_COL_BIGINT;
      } else {
        merged = FILE_SQL_COL_BOOLEAN;
      }
    } else {
      // A temporal column mixed with numbers or booleans cannot keep its type.
      merged = FILE_SQL_COL_VARCHAR;
    }
  }
  return merged;
}

// ---------------------------------------------------------------------------
// path helpers
// ---------------------------------------------------------------------------

int resolve_file_sql_path(ObIAllocator &alloc,
                          const ObString &path,
                          ObString &resolved)
{
  int ret = OB_SUCCESS;
  char buf[PATH_MAX + 1];
  char *real_path = NULL;
  ObCStringHelper helper;
  if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty file path", K(ret));
  } else {
    MEMSET(buf, 0, sizeof(buf));
    // realpath() both normalises the path and resolves symlinks, so a ".."
    // segment can no longer be used to reach outside of secure_file_priv.
    if (NULL == (real_path = ::realpath(helper.convert(path), buf))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("failed to resolve file path", K(ret), K(path), K(errno));
      LOG_USER_ERROR(OB_FILE_NOT_EXIST, path.length(), path.ptr());
    } else {
      ObString tmp(static_cast<int32_t>(strlen(real_path)), real_path);
      if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, tmp, resolved))) {
      }
    }
  }
  return ret;
}

int check_file_sql_enabled(ObBasicSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObObj enable_obj;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL,
                                                    enable_obj))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file sql");
  } else if (0 == enable_obj.get_int()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file sql, set enable_file_sql = on first");
  }
  return ret;
}

int check_file_sql_privilege(ObBasicSessionInfo *session_info,
                             const ObString &path)
{
  int ret = OB_SUCCESS;
  ObObj enable_obj;
  ObString secure_file_priv;
  int64_t enable = 0;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_ENABLE_FILE_SQL, enable_obj))) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file sql");
  } else if (FALSE_IT(enable = enable_obj.get_int())) {
  } else if (0 == enable) {
    // The switch is the first gate; the path check below is the second one.
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file sql, set enable_file_sql = on first");
  } else if (OB_FAIL(session_info->get_secure_file_priv(secure_file_priv))) {
  } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, path))) {
    LOG_WARN("file path is out of secure_file_priv", K(ret), K(path), K(secure_file_priv));
  }
  return ret;
}

int glob_file_sql_paths(ObIAllocator &alloc,
                        const ObString &pattern,
                        ObIArray<ObString> &paths)
{
  int ret = OB_SUCCESS;
  int64_t slash = -1;
  for (int64_t i = pattern.length() - 1; i >= 0 && slash < 0; --i) {
    if ('/' == pattern.ptr()[i]) {
      slash = i;
    }
  }
  char dir_buf[PATH_MAX + 1];
  char pat_buf[PATH_MAX + 1];
  int64_t dir_len = 0;
  int64_t pat_len = 0;
  if (slash < 0) {
    dir_buf[dir_len++] = '.';
    pat_len = pattern.length();
    if (pat_len <= 0 || pat_len > PATH_MAX) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(pat_buf, pattern.ptr(), pat_len);
    }
  } else {
    dir_len = (0 == slash) ? 1 : slash;
    if (dir_len > PATH_MAX) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(dir_buf, pattern.ptr(), dir_len);
      pat_len = pattern.length() - slash - 1;
      if (pat_len > PATH_MAX) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        MEMCPY(pat_buf, pattern.ptr() + slash + 1, pat_len);
      }
    }
  }
  if (OB_SUCC(ret)) {
    dir_buf[dir_len] = '\0';
    pat_buf[pat_len] = '\0';
    DIR *dir = NULL;
    if (NULL == (dir = ::opendir(dir_buf))) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("glob directory is not accessible", K(ret), K(pattern), K(errno));
      LOG_USER_ERROR(OB_FILE_NOT_EXIST, static_cast<int32_t>(dir_len), dir_buf);
    } else {
      struct dirent *entry = NULL;
      while (OB_SUCC(ret) && NULL != (entry = ::readdir(dir))) {
        const char *name = entry->d_name;
        // "." and ".." are never results.  Any other dot file is also skipped
        // unless the pattern itself starts with a dot, which is the behaviour of
        // a shell glob; without this, archives of the test data expose macOS
        // "._name" resource forks through glob('dir/*'). */
        if ('.' == name[0] && ('.' != pat_buf[0] || '\0' == name[1]
                               || ('.' == name[1] && '\0' == name[2]))) {
          continue;
        }
        if (!match_wildcard(pat_buf, name)) {
          continue;
        }
        char full[PATH_MAX + 1];
        const int64_t written = snprintf(full, sizeof(full), "%.*s/%.*s",
                                         static_cast<int>(dir_len), dir_buf,
                                         static_cast<int>(strlen(name)), name);
        if (written <= 0 || written >= static_cast<int64_t>(sizeof(full))) {
          ret = OB_SIZE_OVERFLOW;
        } else {
          struct stat st;
          if (0 != ::stat(full, &st) || !S_ISREG(st.st_mode)) {
            continue;
          }
          ObString match(static_cast<int32_t>(written), full);
          ObString copy;
          if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, match, copy))) {
          } else if (OB_FAIL(paths.push_back(copy))) {
          }
        }
      }
      ::closedir(dir);
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// shared buffered text reader
// ---------------------------------------------------------------------------

ObFileSqlTextReader::ObFileSqlTextReader()
  : file_opened_(false), buf_(NULL), buf_size_(0), data_len_(0), data_pos_(0),
    file_offset_(0), eof_(false)
{
}

ObFileSqlTextReader::~ObFileSqlTextReader()
{
  // Deliberately not virtual: close() is pure virtual in the base class.
  close_text_reader();
}

int ObFileSqlTextReader::init_text_reader(const ObFileSqlTableDef &def)
{
  int ret = OB_SUCCESS;
  // Shallow reference on purpose: the definition (and therefore the path) is
  // owned by the plan, which outlives every execution of this reader.
  path_ = def.path_;
  if (OB_ISNULL(buf_ = static_cast<char *>(ob_malloc(FILE_SQL_READER_BUF_SIZE, "FileSqlRead")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    buf_size_ = FILE_SQL_READER_BUF_SIZE;
    data_len_ = 0;
    data_pos_ = 0;
    file_offset_ = 0;
    eof_ = false;
  }
  return ret;
}

int ObFileSqlTextReader::open_text_reader()
{
  int ret = OB_SUCCESS;
  if (!file_opened_) {
    if (OB_FAIL(file_.open(path_, false))) {
      LOG_WARN("failed to open file", K(ret), K(path_));
    } else {
      file_opened_ = true;
    }
  }
  return ret;
}

void ObFileSqlTextReader::close_text_reader()
{
  if (file_opened_) {
    file_.close();
    file_opened_ = false;
  }
  if (NULL != buf_) {
    ob_free(buf_);
    buf_ = NULL;
  }
  buf_size_ = 0;
  data_len_ = 0;
  data_pos_ = 0;
  file_offset_ = 0;
  eof_ = false;
}

int ObFileSqlTextReader::fill_more()
{
  int ret = OB_SUCCESS;
  if (data_pos_ > 0) {
    const int64_t remain = data_len_ - data_pos_;
    if (remain > 0) {
      MEMMOVE(buf_, buf_ + data_pos_, remain);
    }
    data_len_ = remain;
    data_pos_ = 0;
  }
  if (data_len_ == buf_size_) {
    const int64_t new_size = buf_size_ * 2;
    char *new_buf = static_cast<char *>(ob_malloc(new_size, "FileSqlRead"));
    if (OB_ISNULL(new_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(new_buf, buf_, data_len_);
      ob_free(buf_);
      buf_ = new_buf;
      buf_size_ = new_size;
    }
  }
  if (OB_SUCC(ret) && !eof_) {
    int64_t read_size = 0;
    if (OB_FAIL(file_.pread(buf_ + data_len_, buf_size_ - data_len_,
                            file_offset_, read_size))) {
      LOG_WARN("failed to read file", K(ret), K_(path));
    } else if (0 == read_size) {
      eof_ = true;
    } else {
      data_len_ += read_size;
      file_offset_ += read_size;
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// CSV reader
// ---------------------------------------------------------------------------

ObFileSqlCsvReader::ObFileSqlCsvReader()
  : header_pending_(false), delimiter_(','), quote_('"')
{
}

int ObFileSqlCsvReader::init(const ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  // column_count_ == 0 selects inference mode: every field of the record is
  // returned so that the caller can size the schema.
  column_count_ = def.column_count();
  header_pending_ = (def.header_ && column_count_ > 0);
  delimiter_ = def.delimiter_;
  quote_ = def.quote_;
  if (OB_FAIL(init_text_reader(def))) {
  }
  return ret;
}

int ObFileSqlCsvReader::open()
{
  return open_text_reader();
}

void ObFileSqlCsvReader::close()
{
  close_text_reader();
}

int ObFileSqlCsvReader::parse_record(ObIAllocator &alloc,
                                     ObIArray<ObFileSqlCell> &row,
                                     bool &complete)
{
  int ret = OB_SUCCESS;
  complete = false;
  // Phase 1: find the end of the record.  A newline only terminates the record
  // outside of a quoted field, which is what keeps multi-line values intact.
  int64_t i = data_pos_;
  bool in_quote = false;
  int64_t record_end = -1;
  while (i < data_len_) {
    const char c = buf_[i];
    if (in_quote) {
      if (c == quote_) {
        if (i + 1 < data_len_ && buf_[i + 1] == quote_) {
          i += 2;
        } else {
          in_quote = false;
          ++i;
        }
      } else {
        ++i;
      }
    } else if (c == quote_) {
      in_quote = true;
      ++i;
    } else if (c == '\n') {
      record_end = i + 1;
      break;
    } else {
      ++i;
    }
  }
  if (record_end < 0) {
    if (!eof_) {
      // The record continues in the next chunk.
      return OB_SUCCESS;
    }
    record_end = data_len_;
  }
  int64_t body_end = record_end;
  if (body_end > data_pos_ && '\n' == buf_[body_end - 1]) {
    --body_end;
  }
  if (body_end > data_pos_ && '\r' == buf_[body_end - 1]) {
    --body_end;
  }
  // Phase 2: split the body into fields.
  int64_t p = data_pos_;
  while (OB_SUCC(ret) && p <= body_end) {
    ObFileSqlCell cell;
    const bool at_end = (p >= body_end);
    if (!at_end && buf_[p] == quote_) {
      // Quoted field: a doubled quote inside the quotes is an escaped quote.
      ++p;
      const int64_t out_cap = body_end - p + 1;
      char *out = static_cast<char *>(alloc.alloc(out_cap > 0 ? out_cap : 1));
      if (OB_ISNULL(out)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        int64_t out_len = 0;
        bool closed = false;
        while (p < body_end) {
          if (buf_[p] == quote_) {
            if (p + 1 < body_end && buf_[p + 1] == quote_) {
              out[out_len++] = quote_;
              p += 2;
            } else {
              ++p;
              closed = true;
              break;
            }
          } else {
            out[out_len++] = buf_[p];
            ++p;
          }
        }
        if (!closed) {
          // Unterminated quote: keep the raw text instead of losing data.
          out_len = 0;
        }
        // Whatever sits between the closing quote and the delimiter is dropped.
        while (p < body_end && buf_[p] != delimiter_) {
          ++p;
        }
        cell.is_null_ = false;
        cell.str_.assign_ptr(out, static_cast<int32_t>(out_len));
        if (OB_FAIL(row.push_back(cell))) {
        }
      }
    } else {
      int64_t field_end = p;
      while (field_end < body_end && buf_[field_end] != delimiter_) {
        ++field_end;
      }
      const int32_t len = static_cast<int32_t>(field_end - p);
      cell.str_.assign_ptr(buf_ + p, len);
      // An unquoted empty field is NULL, which is how DuckDB reads CSV.
      cell.is_null_ = (0 == len);
      p = field_end;
      if (OB_FAIL(row.push_back(cell))) {
      }
    }
    if (OB_SUCC(ret)) {
      if (p < body_end && buf_[p] == delimiter_) {
        ++p;
      } else {
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    data_pos_ = record_end;
    complete = true;
  }
  return ret;
}

int ObFileSqlCsvReader::get_next_row(ObIAllocator &alloc, ObIArray<ObFileSqlCell> &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  bool complete = false;
  while (OB_SUCC(ret)) {
    if (data_pos_ >= data_len_ && !eof_ && OB_FAIL(fill_more())) {
    } else if (data_pos_ >= data_len_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(parse_record(alloc, row, complete))) {
    } else if (!complete) {
      if (OB_FAIL(fill_more())) {
      }
    } else if (header_pending_) {
      // The first record holds the column names and is not a data row.
      header_pending_ = false;
      row.reset();
    } else if (row.empty()) {
      // Blank line, keep going.
    } else if (column_count_ > 0 && row.count() != column_count_) {
      // Normalise to the inferred column count: short records are padded with
      // NULL, extra fields are dropped.
      ObFileSqlCell null_cell;
      while (OB_SUCC(ret) && row.count() < column_count_) {
        ret = row.push_back(null_cell);
      }
      while (OB_SUCC(ret) && row.count() > column_count_) {
        row.pop_back();
      }
      if (OB_SUCC(ret)) {
        break;
      }
    } else {
      break;
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// JSON lines parsing (shared by the reader and by schema inference)
// ---------------------------------------------------------------------------

ObFileSqlJsonlParser::ObFileSqlJsonlParser()
{
}

int ObFileSqlJsonlParser::init(const ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  UNUSED(alloc);
  return init_text_reader(def);
}

int ObFileSqlJsonlParser::open()
{
  return open_text_reader();
}

void ObFileSqlJsonlParser::close()
{
  close_text_reader();
}

int ObFileSqlJsonlParser::skip_ws(const char *&p, const char *end)
{
  int ret = OB_SUCCESS;
  while (p < end && (' ' == *p || '\t' == *p || '\r' == *p || '\n' == *p)) {
    ++p;
  }
  return ret;
}

int ObFileSqlJsonlParser::parse_string(ObIAllocator &alloc,
                                       const char *&p,
                                       const char *end,
                                       ObString &str)
{
  int ret = OB_SUCCESS;
  if (p >= end || '"' != *p) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("jsonl: expected a string", K(ret));
  } else {
    ++p;
    char *out = static_cast<char *>(alloc.alloc(end - p + 1));
    int64_t out_len = 0;
    bool closed = false;
    if (OB_ISNULL(out)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      while (OB_SUCC(ret) && p < end) {
        const char c = *p;
        if ('"' == c) {
          ++p;
          closed = true;
          break;
        } else if ('\\' == c) {
          ++p;
          if (p >= end) {
            ret = OB_ERR_PARSER_SYNTAX;
          } else {
            const char esc = *p++;
            switch (esc) {
              case '"': out[out_len++] = '"'; break;
              case '\\': out[out_len++] = '\\'; break;
              case '/': out[out_len++] = '/'; break;
              case 'b': out[out_len++] = '\b'; break;
              case 'f': out[out_len++] = '\f'; break;
              case 'n': out[out_len++] = '\n'; break;
              case 'r': out[out_len++] = '\r'; break;
              case 't': out[out_len++] = '\t'; break;
              case 'u': {
                if (p + 4 > end) {
                  ret = OB_ERR_PARSER_SYNTAX;
                } else {
                  int32_t code = 0;
                  for (int64_t i = 0; i < 4; ++i) {
                    const char h = p[i];
                    code <<= 4;
                    if (h >= '0' && h <= '9') {
                      code |= (h - '0');
                    } else if (h >= 'a' && h <= 'f') {
                      code |= (h - 'a' + 10);
                    } else if (h >= 'A' && h <= 'F') {
                      code |= (h - 'A' + 10);
                    } else {
                      ret = OB_ERR_PARSER_SYNTAX;
                    }
                  }
                  p += 4;
                  if (OB_SUCC(ret)) {
                    if (code < 0x80) {
                      out[out_len++] = static_cast<char>(code);
                    } else if (code < 0x800) {
                      out[out_len++] = static_cast<char>(0xC0 | (code >> 6));
                      out[out_len++] = static_cast<char>(0x80 | (code & 0x3F));
                    } else {
                      out[out_len++] = static_cast<char>(0xE0 | (code >> 12));
                      out[out_len++] = static_cast<char>(0x80 | ((code >> 6) & 0x3F));
                      out[out_len++] = static_cast<char>(0x80 | (code & 0x3F));
                    }
                  }
                }
                break;
              }
              default: {
                ret = OB_ERR_PARSER_SYNTAX;
                break;
              }
            }
          }
        } else {
          out[out_len++] = c;
          ++p;
        }
      }
      if (OB_SUCC(ret) && !closed) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("jsonl: unterminated string", K(ret));
      }
      if (OB_SUCC(ret)) {
        str.assign_ptr(out, static_cast<int32_t>(out_len));
      }
    }
  }
  return ret;
}

int ObFileSqlJsonlParser::parse_value(ObIAllocator &alloc,
                                      const char *&p,
                                      const char *end,
                                      ObFileSqlJsonValue &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(skip_ws(p, end))) {
  } else if (p >= end) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("jsonl: unexpected end of record", K(ret));
  } else if ('"' == *p) {
    value.kind_ = ObFileSqlJsonValue::J_STRING;
    if (OB_FAIL(parse_string(alloc, p, end, value.str_))) {
    }
  } else if ('t' == *p && end - p >= 4 && 0 == strncmp(p, "true", 4)) {
    value.kind_ = ObFileSqlJsonValue::J_BOOL;
    value.int_value_ = 1;
    p += 4;
  } else if ('f' == *p && end - p >= 5 && 0 == strncmp(p, "false", 5)) {
    value.kind_ = ObFileSqlJsonValue::J_BOOL;
    value.int_value_ = 0;
    p += 5;
  } else if ('n' == *p && end - p >= 4 && 0 == strncmp(p, "null", 4)) {
    value.kind_ = ObFileSqlJsonValue::J_NULL;
    p += 4;
  } else if ('[' == *p || '{' == *p) {
    // Only flat objects are part of the newline-delimited schema.
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested object or array in newline delimited json");
  } else {
    const char *start = p;
    while (p < end && ('-' == *p || '+' == *p || '.' == *p || 'e' == *p || 'E' == *p ||
                       (*p >= '0' && *p <= '9'))) {
      ++p;
    }
    if (p == start) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("jsonl: bad value", K(ret));
    } else {
      const ObString num(static_cast<int32_t>(p - start), start);
      int64_t int_value = 0;
      double double_value = 0;
      if (file_sql_parse_int(num, int_value)) {
        value.kind_ = ObFileSqlJsonValue::J_INT;
        value.int_value_ = int_value;
      } else if (file_sql_parse_double(num, double_value)) {
        value.kind_ = ObFileSqlJsonValue::J_DOUBLE;
        value.double_value_ = double_value;
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("jsonl: bad number", K(ret));
      }
    }
  }
  return ret;
}

int ObFileSqlJsonlParser::next_record(ObIAllocator &alloc,
                                      ObIArray<ObFileSqlJsonField> &fields)
{
  int ret = OB_SUCCESS;
  fields.reset();
  ObString line;
  while (OB_SUCC(ret) && line.empty()) {
    bool has_line = false;
    if (OB_FAIL(read_line(alloc, line, has_line))) {
    } else if (!has_line) {
      ret = OB_ITER_END;
    }
  }
  const char *p = line.ptr();
  const char *end = line.ptr() + line.length();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(skip_ws(p, end))) {
  } else if (p >= end || '{' != *p) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("jsonl: record is not an object", K(ret));
  } else {
    ++p;
    bool first = true;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      ObString key;
      if (OB_FAIL(skip_ws(p, end))) {
      } else if (p >= end) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("jsonl: unterminated object", K(ret));
      } else if ('}' == *p) {
        ++p;
        done = true;
      } else if (first) {
        if (OB_FAIL(parse_string(alloc, p, end, key))) {
        }
      } else if (',' != *p) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("jsonl: expected ','", K(ret));
      } else {
        ++p;
        if (OB_FAIL(skip_ws(p, end))) {
        } else if (p < end && '}' == *p) {
          ++p;
          done = true;
        } else if (OB_FAIL(parse_string(alloc, p, end, key))) {
        }
      }
      if (OB_FAIL(ret) || done) {
      } else {
        first = false;
        ObFileSqlJsonField field;
        if (OB_FAIL(skip_ws(p, end))) {
        } else if (p >= end || ':' != *p) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WARN("jsonl: expected ':'", K(ret));
        } else {
          ++p;
          if (OB_FAIL(parse_value(alloc, p, end, field.value_))) {
          } else {
            field.key_ = key;
            if (OB_FAIL(fields.push_back(field))) {
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFileSqlJsonlParser::read_line(ObIAllocator &alloc, ObString &line, bool &has_line)
{
  int ret = OB_SUCCESS;
  has_line = false;
  line.reset();
  while (OB_SUCC(ret) && !has_line) {
    if (data_pos_ >= data_len_ && !eof_) {
      if (OB_FAIL(fill_more())) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (data_pos_ >= data_len_) {
      break;
    } else {
      int64_t end = data_pos_;
      while (end < data_len_ && '\n' != buf_[end]) {
        ++end;
      }
      if (end >= data_len_ && !eof_) {
        // The line continues in the next chunk.
        if (OB_FAIL(fill_more())) {
        }
      } else {
        int64_t body_end = end;
        if (body_end > data_pos_ && '\r' == buf_[body_end - 1]) {
          --body_end;
        }
        const int32_t len = static_cast<int32_t>(body_end - data_pos_);
        char *copy = static_cast<char *>(alloc.alloc(len > 0 ? len : 1));
        if (OB_ISNULL(copy)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          if (len > 0) {
            MEMCPY(copy, buf_ + data_pos_, len);
          }
          line.assign_ptr(copy, len);
          has_line = (len > 0);
          data_pos_ = (end < data_len_ ? end + 1 : data_len_);
        }
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// JSON lines reader
// ---------------------------------------------------------------------------

int ObFileSqlJsonlReader::init(const ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  column_count_ = def.column_count();
  for (int64_t i = 0; OB_SUCC(ret) && i < def.column_names_.count(); ++i) {
    if (OB_FAIL(keys_.push_back(def.column_names_.at(i)))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parser_.init(def, alloc))) {
  }
  return ret;
}

int ObFileSqlJsonlReader::open()
{
  return parser_.open();
}

void ObFileSqlJsonlReader::close()
{
  parser_.close();
}

int ObFileSqlJsonlReader::get_next_row(ObIAllocator &alloc, ObIArray<ObFileSqlCell> &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  ObFileSqlCell null_cell;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
    if (OB_FAIL(row.push_back(null_cell))) {
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parser_.next_record(alloc, fields_))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < fields_.count(); ++i) {
        const ObFileSqlJsonField &field = fields_.at(i);
        for (int64_t col = 0; OB_SUCC(ret) && col < keys_.count(); ++col) {
          if (ObCharset::case_insensitive_equal(keys_.at(col), field.key_)) {
            if (OB_FAIL(json_value_to_cell(alloc, field.value_, row.at(col)))) {
            }
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObFileSqlJsonlReader::json_value_to_cell(ObIAllocator &alloc,
                                             const ObFileSqlJsonValue &value,
                                             ObFileSqlCell &cell)
{
  int ret = OB_SUCCESS;
  if (ObFileSqlJsonValue::J_NULL == value.kind_) {
    cell.is_null_ = true;
  } else {
    cell.is_null_ = false;
    char tmp[64];
    int32_t len = 0;
    const char *text = NULL;
    if (ObFileSqlJsonValue::J_BOOL == value.kind_) {
      text = (1 == value.int_value_) ? "true" : "false";
      len = static_cast<int32_t>(strlen(text));
    } else if (ObFileSqlJsonValue::J_INT == value.kind_) {
      len = snprintf(tmp, sizeof(tmp), "%ld", value.int_value_);
      text = tmp;
    } else if (ObFileSqlJsonValue::J_DOUBLE == value.kind_) {
      len = snprintf(tmp, sizeof(tmp), "%.17g", value.double_value_);
      text = tmp;
    } else {
      cell.str_ = value.str_;
    }
    if (OB_SUCC(ret) && NULL != text) {
      char *copy = static_cast<char *>(alloc.alloc(len > 0 ? len : 1));
      if (OB_ISNULL(copy)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(copy, text, len);
        cell.str_.assign_ptr(copy, len);
      }
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// glob reader
// ---------------------------------------------------------------------------

int ObFileSqlGlobReader::init(const ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  column_count_ = 1;
  if (OB_FAIL(glob_file_sql_paths(alloc, def.path_, files_))) {
  }
  return ret;
}

int ObFileSqlGlobReader::open()
{
  pos_ = 0;
  return OB_SUCCESS;
}

void ObFileSqlGlobReader::close()
{
  pos_ = 0;
}

int ObFileSqlGlobReader::get_next_row(ObIAllocator &alloc, ObIArray<ObFileSqlCell> &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  ObFileSqlCell cell;
  if (pos_ >= files_.count()) {
    ret = OB_ITER_END;
  } else {
    cell.is_null_ = false;
    if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, files_.at(pos_), cell.str_))) {
    } else if (OB_FAIL(row.push_back(cell))) {
    } else {
      ++pos_;
    }
  }
  return ret;
}

// ---------------------------------------------------------------------------
// factory + inference
// ---------------------------------------------------------------------------

int create_file_sql_reader(const ObFileSqlTableDef &def,
                           ObIAllocator &alloc,
                           ObFileSqlRowReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = NULL;
  void *buf = NULL;
  if (FILE_SQL_CSV == def.format_) {
    ObFileSqlCsvReader *csv_reader = NULL;
    if (OB_ISNULL(buf = alloc.alloc(sizeof(ObFileSqlCsvReader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      csv_reader = new (buf) ObFileSqlCsvReader();
      if (OB_FAIL(csv_reader->init(def, alloc))) {
      } else {
        reader = csv_reader;
      }
    }
  } else if (FILE_SQL_JSONL == def.format_) {
    ObFileSqlJsonlReader *json_reader = NULL;
    if (OB_ISNULL(buf = alloc.alloc(sizeof(ObFileSqlJsonlReader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      json_reader = new (buf) ObFileSqlJsonlReader();
      if (OB_FAIL(json_reader->init(def, alloc))) {
      } else {
        reader = json_reader;
      }
    }
  } else if (FILE_SQL_GLOB == def.format_) {
    ObFileSqlGlobReader *glob_reader = NULL;
    if (OB_ISNULL(buf = alloc.alloc(sizeof(ObFileSqlGlobReader)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      glob_reader = new (buf) ObFileSqlGlobReader();
      if (OB_FAIL(glob_reader->init(def, alloc))) {
      } else {
        reader = glob_reader;
      }
    }
  } else if (FILE_SQL_PARQUET == def.format_) {
    if (OB_FAIL(create_parquet_file_sql_reader(def, alloc, reader))) {
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unknown file sql format", K(ret), K(def.format_));
  }
  if (OB_FAIL(ret) && NULL != reader) {
    reader->close();
    reader = NULL;
  }
  return ret;
}

int infer_csv_or_jsonl_schema(ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObFileSqlRowReader *reader = NULL;
  ObArenaAllocator row_alloc("FileSqlInfer");
  ObSEArray<int64_t, 16, ModulePageAllocator, true> col_types;
  ObSEArray<bool, 16, ModulePageAllocator, true> col_nullable;
  ObSEArray<ObString, 16, ModulePageAllocator, true> col_names;
  int64_t row_cnt = 0;
  // A definition without columns puts the CSV reader into inference mode, which
  // returns every field of a record instead of padding/truncating it.
  if (OB_FAIL(create_file_sql_reader(def, alloc, reader))) {
  } else if (OB_FAIL(reader->open())) {
  } else {
    ObSEArray<ObFileSqlCell, 32, ModulePageAllocator, true> fields;
    bool first_row = true;
    while (OB_SUCC(ret)) {
      row_alloc.reuse();
      fields.reset();
      if (OB_FAIL(reader->get_next_row(row_alloc, fields))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (first_row && FILE_SQL_CSV == def.format_ && def.header_) {
        first_row = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
          ObString name;
          if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, fields.at(i).str_, name))) {
          } else if (OB_FAIL(col_names.push_back(name))) {
          } else if (OB_FAIL(col_types.push_back(FILE_SQL_COL_NULL))) {
          } else if (OB_FAIL(col_nullable.push_back(false))) {
          }
        }
        // Duplicate header names get a "__N" suffix, which is what makes
        // `select id, id__2 from ...` addressable.
        for (int64_t i = 0; OB_SUCC(ret) && i < col_names.count(); ++i) {
          int64_t dup = 1;
          for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
            if (col_names.at(j) == col_names.at(i)) {
              ++dup;
            }
          }
          if (dup > 1) {
            ObSqlString suffix;
            ObString renamed;
            if (OB_FAIL(suffix.append_fmt("%.*s__%ld", col_names.at(i).length(),
                                          col_names.at(i).ptr(), dup))) {
            } else if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(
                         alloc, ObString(suffix.length(), suffix.ptr()), renamed))) {
            } else {
              col_names.at(i) = renamed;
            }
          }
        }
      } else {
        first_row = false;
        if (fields.count() > col_names.count()) {
          // A record wider than the header (or the first record when there is
          // no header) defines the column set.
          for (int64_t i = col_names.count(); OB_SUCC(ret) && i < fields.count(); ++i) {
            ObSqlString gen_name;
            ObString name;
            if (OB_FAIL(gen_name.append_fmt("column%ld", i))) {
            } else if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(
                         alloc, ObString(gen_name.length(), gen_name.ptr()), name))) {
            } else if (OB_FAIL(col_names.push_back(name))) {
            } else if (OB_FAIL(col_types.push_back(FILE_SQL_COL_NULL))) {
            } else if (OB_FAIL(col_nullable.push_back(false))) {
            }
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < col_types.count(); ++i) {
          int64_t detected = FILE_SQL_COL_NULL;
          if (i < fields.count() && !fields.at(i).is_null_) {
            if (OB_FAIL(file_sql_detect_type(fields.at(i).str_, detected))) {
            }
          }
          if (OB_SUCC(ret)) {
            col_types.at(i) = file_sql_merge_type(col_types.at(i), detected);
            if (FILE_SQL_COL_NULL == detected) {
              col_nullable.at(i) = true;
            }
          }
        }
        ++row_cnt;
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_names.count(); ++i) {
        if (OB_FAIL(def.column_names_.push_back(col_names.at(i)))) {
        } else if (OB_FAIL(def.column_types_.push_back(col_types.at(i)))) {
        } else if (OB_FAIL(def.column_nullable_.push_back(col_nullable.at(i)))) {
        }
      }
      if (OB_SUCC(ret)) {
        def.row_cnt_ = row_cnt;
      }
    }
  }
  if (NULL != reader) {
    reader->close();
  }
  return ret;
}

int infer_jsonl_schema(ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  ObFileSqlJsonlParser *parser = NULL;
  ObArenaAllocator row_alloc("FileSqlJsonl");
  int64_t row_cnt = 0;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ObFileSqlJsonlParser)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    parser = new (buf) ObFileSqlJsonlParser();
    if (OB_FAIL(parser->init(def, alloc))) {
    } else if (OB_FAIL(parser->open())) {
    } else {
      ObSEArray<ObFileSqlJsonField, 16, ModulePageAllocator, true> fields;
      while (OB_SUCC(ret)) {
        row_alloc.reuse();
        fields.reset();
        if (OB_FAIL(parser->next_record(row_alloc, fields))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
            const ObFileSqlJsonField &field = fields.at(i);
            int64_t detected = FILE_SQL_COL_NULL;
            int64_t col = -1;
            for (int64_t j = 0; OB_SUCC(ret) && col < 0 && j < def.column_names_.count(); ++j) {
              if (ObCharset::case_insensitive_equal(def.column_names_.at(j), field.key_)) {
                col = j;
              }
            }
            if (ObFileSqlJsonValue::J_NULL != field.value_.kind_) {
              switch (field.value_.kind_) {
                case ObFileSqlJsonValue::J_BOOL: detected = FILE_SQL_COL_BOOLEAN; break;
                case ObFileSqlJsonValue::J_INT: detected = FILE_SQL_COL_BIGINT; break;
                case ObFileSqlJsonValue::J_DOUBLE: detected = FILE_SQL_COL_DOUBLE; break;
                default: detected = FILE_SQL_COL_VARCHAR; break;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (col >= 0) {
              def.column_types_.at(col) = file_sql_merge_type(def.column_types_.at(col), detected);
            } else {
              ObString name;
              if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, field.key_, name))) {
              } else if (OB_FAIL(def.column_names_.push_back(name))) {
              } else if (OB_FAIL(def.column_types_.push_back(detected))) {
              } else if (OB_FAIL(def.column_nullable_.push_back(FALSE))) {
              }
            }
          }
          // Every record marks the columns it does not mention as nullable.
          for (int64_t j = 0; OB_SUCC(ret) && j < def.column_names_.count(); ++j) {
            bool present = false;
            for (int64_t i = 0; OB_SUCC(ret) && !present && i < fields.count(); ++i) {
              present = ObCharset::case_insensitive_equal(def.column_names_.at(j),
                                                          fields.at(i).key_);
            }
            if (OB_SUCC(ret) && !present) {
              def.column_nullable_.at(j) = true;
            }
          }
          ++row_cnt;
        }
      }
    }
    if (NULL != parser) {
      parser->close();
    }
  }
  if (OB_SUCC(ret)) {
    def.row_cnt_ = row_cnt;
  }
  return ret;
}

int infer_file_sql_schema(ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (FILE_SQL_GLOB == def.format_) {
    // glob() performs discovery only; it always exposes a single `file` column.
    ObString name;
    if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, ObString("file"), name))) {
    } else if (OB_FAIL(def.column_names_.push_back(name))) {
    } else if (OB_FAIL(def.column_types_.push_back(FILE_SQL_COL_VARCHAR))) {
    } else if (OB_FAIL(def.column_nullable_.push_back(false))) {
    } else {
      def.row_cnt_ = 0;
    }
  } else if (FILE_SQL_PARQUET == def.format_) {
    if (OB_FAIL(infer_parquet_file_sql_schema(def, alloc))) {
    }
  } else if (FILE_SQL_JSONL == def.format_) {
    if (OB_FAIL(infer_jsonl_schema(def, alloc))) {
    }
  } else if (FILE_SQL_CSV == def.format_) {
    if (OB_FAIL(infer_csv_or_jsonl_schema(def, alloc))) {
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unknown file sql format", K(ret), K(def.format_));
  }
  if (OB_FAIL(ret)) {
  } else {
    // A column that only ever held NULL is exposed as VARCHAR.
    for (int64_t i = 0; i < def.column_types_.count(); ++i) {
      if (FILE_SQL_COL_NULL == def.column_types_.at(i)) {
        def.column_types_.at(i) = FILE_SQL_COL_VARCHAR;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
