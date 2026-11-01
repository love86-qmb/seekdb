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

// A parquet reader written directly against the file format.  The footer is a
// Thrift struct serialised with the compact protocol, a column chunk is a
// sequence of pages, and page values are either PLAIN or dictionary encoded;
// the definition levels and the dictionary indices both use the RLE /
// bit-packing hybrid encoding.  Nothing here depends on the shape of one
// particular file.

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_file_sql_parquet.h"

#include <stdint.h>
#include <string.h>
#include <sys/stat.h>

#include "common/timezone/ob_time_convert.h"
#include "lib/charset/ob_charset.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/basic/ob_file_sql_reader.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

namespace
{

// ---- format constants -----------------------------------------------------
enum ObParquetPhysicalType
{
  PARQUET_BOOLEAN = 0,
  PARQUET_INT32 = 1,
  PARQUET_INT64 = 2,
  PARQUET_INT96 = 3,
  PARQUET_FLOAT = 4,
  PARQUET_DOUBLE = 5,
  PARQUET_BYTE_ARRAY = 6,
  PARQUET_FIXED_LEN_BYTE_ARRAY = 7,
};

enum ObParquetPageType
{
  PARQUET_PAGE_DATA = 0,
  PARQUET_PAGE_INDEX = 1,
  PARQUET_PAGE_DICTIONARY = 2,
  PARQUET_PAGE_DATA_V2 = 3,
};

enum ObParquetEncoding
{
  PARQUET_ENC_PLAIN = 0,
  PARQUET_ENC_PLAIN_DICTIONARY = 2,
  PARQUET_ENC_RLE_DICTIONARY = 8,
};

enum ObParquetCodec
{
  PARQUET_CODEC_UNCOMPRESSED = 0,
  PARQUET_CODEC_SNAPPY = 1,
};

enum ObParquetConvertedType
{
  PARQUET_CONVERTED_UTF8 = 0,
  PARQUET_CONVERTED_DATE = 6,
  PARQUET_CONVERTED_TIMESTAMP_MILLIS = 9,
  PARQUET_CONVERTED_TIMESTAMP_MICROS = 10,
};

enum ObParquetRepetition
{
  PARQUET_REP_REQUIRED = 0,
  PARQUET_REP_OPTIONAL = 1,
  PARQUET_REP_REPEATED = 2,
};

// Thrift compact protocol type tags.
enum ObThriftType
{
  THRIFT_STOP = 0,
  THRIFT_BOOL_TRUE = 1,
  THRIFT_BOOL_FALSE = 2,
  THRIFT_BYTE = 3,
  THRIFT_I16 = 4,
  THRIFT_I32 = 5,
  THRIFT_I64 = 6,
  THRIFT_DOUBLE = 7,
  THRIFT_BINARY = 8,
  THRIFT_LIST = 9,
  THRIFT_SET = 10,
  THRIFT_MAP = 11,
  THRIFT_STRUCT = 12,
};

const int64_t PARQUET_MAGIC_LEN = 4;
const int64_t PARQUET_FOOTER_LEN = 8;
// A page header is a small Thrift struct; this is the window read to parse it.
const int64_t PARQUET_PAGE_HEADER_READ_LEN = 4096;

inline int bit_width_of(int64_t max_value)
{
  int width = 0;
  while (max_value > 0) {
    ++width;
    max_value >>= 1;
  }
  return width;
}

inline int64_t read_le(const char *p, const int64_t bytes)
{
  int64_t value = 0;
  for (int64_t i = bytes - 1; i >= 0; --i) {
    value = (value << 8) | static_cast<unsigned char>(p[i]);
  }
  return value;
}

// ---- Thrift compact protocol ----------------------------------------------
// Only the subset parquet uses is implemented.
class ObParquetThriftReader
{
public:
  ObParquetThriftReader() : buf_(NULL), len_(0), pos_(0), last_field_(0) {}
  void reset(const char *buf, const int64_t len)
  {
    buf_ = buf;
    len_ = len;
    pos_ = 0;
    last_field_ = 0;
  }
  int64_t pos() const { return pos_; }

  // Saves the delta base of the enclosing struct and starts a new one.
  int32_t begin_struct()
  {
    const int32_t saved = last_field_;
    last_field_ = 0;
    return saved;
  }
  void end_struct(const int32_t saved) { last_field_ = saved; }

  int read_field(int32_t &field_id, int8_t &type)
  {
    int ret = OB_SUCCESS;
    int8_t byte = 0;
    if (OB_FAIL(read_byte(byte))) {
    } else {
      type = byte & 0x0f;
      if (THRIFT_STOP == type) {
        field_id = 0;
      } else {
        const int8_t delta = (byte >> 4) & 0x0f;
        if (0 != delta) {
          field_id = last_field_ + delta;
        } else if (OB_FAIL(read_zigzag(reinterpret_cast<int64_t &>(field_id)))) {
        }
        last_field_ = field_id;
      }
    }
    return ret;
  }

  int read_i32(int32_t &value)
  {
    int ret = OB_SUCCESS;
    int64_t tmp = 0;
    if (OB_FAIL(read_zigzag(tmp))) {
    } else {
      value = static_cast<int32_t>(tmp);
    }
    return ret;
  }
  int read_i64(int64_t &value) { return read_zigzag(value); }

  int read_double(double &value)
  {
    int ret = OB_SUCCESS;
    if (pos_ + 8 > len_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      MEMCPY(&value, buf_ + pos_, 8);
      pos_ += 8;
    }
    return ret;
  }

  int read_binary(ObString &value)
  {
    int ret = OB_SUCCESS;
    uint64_t size = 0;
    if (OB_FAIL(read_varint(size))) {
    } else if (OB_UNLIKELY(pos_ + static_cast<int64_t>(size) > len_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value.assign_ptr(buf_ + pos_, static_cast<int32_t>(size));
      pos_ += size;
    }
    return ret;
  }

  int read_list_header(int8_t &elem_type, int64_t &size)
  {
    int ret = OB_SUCCESS;
    int8_t byte = 0;
    if (OB_FAIL(read_byte(byte))) {
    } else {
      elem_type = byte & 0x0f;
      int64_t short_size = (byte >> 4) & 0x0f;
      if (15 == short_size) {
        uint64_t long_size = 0;
        if (OB_FAIL(read_varint(long_size))) {
        } else {
          short_size = static_cast<int64_t>(long_size);
        }
      }
      size = short_size;
    }
    return ret;
  }

  int skip(const int8_t type)
  {
    int ret = OB_SUCCESS;
    switch (type) {
      case THRIFT_BOOL_TRUE:
      case THRIFT_BOOL_FALSE:
        break;
      case THRIFT_BYTE: {
        int8_t tmp = 0;
        ret = read_byte(tmp);
        break;
      }
      case THRIFT_I16:
      case THRIFT_I32:
      case THRIFT_I64: {
        int64_t tmp = 0;
        ret = read_zigzag(tmp);
        break;
      }
      case THRIFT_DOUBLE: {
        double tmp = 0;
        ret = read_double(tmp);
        break;
      }
      case THRIFT_BINARY: {
        ObString tmp;
        ret = read_binary(tmp);
        break;
      }
      case THRIFT_LIST:
      case THRIFT_SET: {
        int8_t elem_type = 0;
        int64_t size = 0;
        if (OB_FAIL(read_list_header(elem_type, size))) {
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
          ret = skip(elem_type);
        }
        break;
      }
      case THRIFT_MAP: {
        uint64_t size = 0;
        int8_t byte = 0;
        if (OB_FAIL(read_varint(size))) {
        } else if (0 != size) {
          if (OB_FAIL(read_byte(byte))) {
          } else {
            const int8_t key_type = (byte >> 4) & 0x0f;
            const int8_t value_type = byte & 0x0f;
            for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
              if (OB_FAIL(skip(key_type))) {
              } else {
                ret = skip(value_type);
              }
            }
          }
        }
        break;
      }
      case THRIFT_STRUCT: {
        const int32_t saved = last_field_;
        last_field_ = 0;
        bool done = false;
        while (OB_SUCC(ret) && !done) {
          int32_t field_id = 0;
          int8_t field_type = 0;
          if (OB_FAIL(read_field(field_id, field_type))) {
          } else if (THRIFT_STOP == field_type) {
            done = true;
          } else {
            ret = skip(field_type);
          }
        }
        last_field_ = saved;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }
    return ret;
  }

private:
  int read_byte(int8_t &value)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= len_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      value = buf_[pos_++];
    }
    return ret;
  }
  int read_varint(uint64_t &value)
  {
    int ret = OB_SUCCESS;
    value = 0;
    int shift = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      int8_t byte = 0;
      if (OB_FAIL(read_byte(byte))) {
      } else {
        value |= (static_cast<uint64_t>(byte) & 0x7f) << shift;
        if (0 == (byte & 0x80)) {
          done = true;
        } else {
          shift += 7;
          if (shift > 63) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
    return ret;
  }
  int read_zigzag(int64_t &value)
  {
    int ret = OB_SUCCESS;
    uint64_t raw = 0;
    if (OB_FAIL(read_varint(raw))) {
    } else {
      value = static_cast<int64_t>(raw >> 1) ^ -static_cast<int64_t>(raw & 1);
    }
    return ret;
  }
  const char *buf_;
  int64_t len_;
  int64_t pos_;
  int32_t last_field_;
};

// ---- footer structures ----------------------------------------------------
struct ObParquetLeaf
{
  ObParquetLeaf()
    : name_(), physical_type_(PARQUET_INT32), converted_type_(-1),
      max_def_level_(0), nullable_(false) {}
  ObString name_;
  int32_t physical_type_;
  int32_t converted_type_;
  int32_t max_def_level_;
  bool nullable_;
};

struct ObParquetChunkMeta
{
  ObParquetChunkMeta()
    : physical_type_(PARQUET_INT32), codec_(PARQUET_CODEC_UNCOMPRESSED), num_values_(0),
      data_page_offset_(-1), dict_page_offset_(-1) {}
  int32_t physical_type_;
  int32_t codec_;
  int64_t num_values_;
  int64_t data_page_offset_;
  int64_t dict_page_offset_;
};

struct ObParquetRowGroupMeta
{
  ObParquetRowGroupMeta() : columns_(), num_rows_(0) {}
  ObSEArray<ObParquetChunkMeta, 8, ModulePageAllocator, true> columns_;
  int64_t num_rows_;
};

struct ObParquetFileMeta
{
  ObParquetFileMeta() : leaves_(), row_groups_(), num_rows_(0) {}
  ObSEArray<ObParquetLeaf, 8, ModulePageAllocator, true> leaves_;
  ObSEArray<ObParquetRowGroupMeta *, 4, ModulePageAllocator, true> row_groups_;
  int64_t num_rows_;
};

// ---- snappy ---------------------------------------------------------------
// The raw snappy block format (not the framed one), which is what parquet uses.
int snappy_decompress(const char *in, const int64_t in_len,
                      char *out, const int64_t out_cap, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  int64_t ip = 0;
  int64_t op = 0;
  out_len = 0;
  // The stream starts with the uncompressed length as a varint.
  uint64_t expected = 0;
  int shift = 0;
  bool got_len = false;
  while (OB_SUCC(ret) && !got_len) {
    if (ip >= in_len) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const unsigned char byte = static_cast<unsigned char>(in[ip++]);
      expected |= (static_cast<uint64_t>(byte) & 0x7f) << shift;
      if (0 == (byte & 0x80)) {
        got_len = true;
      } else {
        shift += 7;
        if (shift > 63) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (static_cast<int64_t>(expected) > out_cap) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    while (OB_SUCC(ret) && ip < in_len) {
      const unsigned char tag = static_cast<unsigned char>(in[ip++]);
      const int kind = tag & 0x03;
      if (0 == kind) {
        int64_t len = (tag >> 2) + 1;
        if (len > 60) {
          const int64_t extra = len - 60;
          if (ip + extra > in_len) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          len = 0;
          for (int64_t i = 0; i < extra; ++i) {
            len |= static_cast<int64_t>(static_cast<unsigned char>(in[ip + i])) << (8 * i);
          }
          len += 1;
          ip += extra;
        }
        if (ip + len > in_len || op + len > out_cap) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        MEMCPY(out + op, in + ip, len);
        ip += len;
        op += len;
      } else {
        int64_t len = 0;
        int64_t offset = 0;
        if (1 == kind) {
          len = 4 + ((tag >> 2) & 0x07);
          if (ip + 1 > in_len) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          offset = ((tag >> 5) << 8) | static_cast<unsigned char>(in[ip]);
          ip += 1;
        } else if (2 == kind) {
          len = (tag >> 2) + 1;
          if (ip + 2 > in_len) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          offset = static_cast<unsigned char>(in[ip]) |
                   (static_cast<int64_t>(static_cast<unsigned char>(in[ip + 1])) << 8);
          ip += 2;
        } else {
          len = (tag >> 2) + 1;
          if (ip + 4 > in_len) {
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          offset = read_le(in + ip, 4);
          ip += 4;
        }
        if (offset <= 0 || offset > op || op + len > out_cap) {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        // The copy may overlap the output, so it has to be byte wise.
        for (int64_t i = 0; i < len; ++i) {
          out[op] = out[op - offset];
          ++op;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (static_cast<int64_t>(expected) != op) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        out_len = op;
      }
    }
  }
  return ret;
}

// ---- footer parsing -------------------------------------------------------
int parse_schema_element(ObParquetThriftReader &reader,
                         ObString &name, int32_t &physical_type,
                         int32_t &repetition, int32_t &num_children,
                         int32_t &converted_type)
{
  int ret = OB_SUCCESS;
  physical_type = -1;
  repetition = PARQUET_REP_REQUIRED;
  num_children = 0;
  converted_type = -1;
  const int32_t saved_base = reader.begin_struct();
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t field_id = 0;
    int8_t type = 0;
    if (OB_FAIL(reader.read_field(field_id, type))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (1 == field_id) {
      ret = reader.read_i32(physical_type);
    } else if (3 == field_id) {
      ret = reader.read_i32(repetition);
    } else if (4 == field_id) {
      ret = reader.read_binary(name);
    } else if (5 == field_id) {
      ret = reader.read_i32(num_children);
    } else if (6 == field_id) {
      ret = reader.read_i32(converted_type);
    } else {
      ret = reader.skip(type);
    }
  }
  reader.end_struct(saved_base);
  return ret;
}

int parse_column_meta(ObParquetThriftReader &reader, ObParquetChunkMeta &chunk)
{
  int ret = OB_SUCCESS;
  const int32_t saved_base = reader.begin_struct();
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t field_id = 0;
    int8_t type = 0;
    if (OB_FAIL(reader.read_field(field_id, type))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (1 == field_id) {
      ret = reader.read_i32(chunk.physical_type_);
    } else if (4 == field_id) {
      ret = reader.read_i32(chunk.codec_);
    } else if (5 == field_id) {
      ret = reader.read_i64(chunk.num_values_);
    } else if (9 == field_id) {
      ret = reader.read_i64(chunk.data_page_offset_);
    } else if (11 == field_id) {
      ret = reader.read_i64(chunk.dict_page_offset_);
    } else {
      ret = reader.skip(type);
    }
  }
  reader.end_struct(saved_base);
  return ret;
}

int parse_row_group(ObParquetThriftReader &reader, ObIAllocator &alloc,
                    ObParquetRowGroupMeta *&row_group)
{
  int ret = OB_SUCCESS;
  row_group = NULL;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(ObParquetRowGroupMeta)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    row_group = new (buf) ObParquetRowGroupMeta();
    const int32_t saved_base = reader.begin_struct();
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      int32_t field_id = 0;
      int8_t type = 0;
      if (OB_FAIL(reader.read_field(field_id, type))) {
      } else if (THRIFT_STOP == type) {
        done = true;
      } else if (1 == field_id) {
        int8_t elem_type = 0;
        int64_t column_count = 0;
        if (OB_FAIL(reader.read_list_header(elem_type, column_count))) {
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
          bool chunk_done = false;
          const int32_t saved_chunk_base = reader.begin_struct();
          ObParquetChunkMeta chunk;
          while (OB_SUCC(ret) && !chunk_done) {
            int32_t chunk_field = 0;
            int8_t chunk_type = 0;
            if (OB_FAIL(reader.read_field(chunk_field, chunk_type))) {
            } else if (THRIFT_STOP == chunk_type) {
              chunk_done = true;
            } else if (3 == chunk_field) {
              // ColumnChunk.meta_data
              ret = parse_column_meta(reader, chunk);
            } else {
              ret = reader.skip(chunk_type);
            }
          }
          reader.end_struct(saved_chunk_base);
          if (OB_SUCC(ret) && OB_FAIL(row_group->columns_.push_back(chunk))) {
          }
        }
      } else if (3 == field_id) {
        ret = reader.read_i64(row_group->num_rows_);
      } else {
        ret = reader.skip(type);
      }
    }
    reader.end_struct(saved_base);
  }
  return ret;
}

// Flattens the schema: this reader supports the flat layout (a root plus leaf
// columns), which is what a table function exposes.
int parse_file_meta(ObIAllocator &alloc, const char *buf, const int64_t len,
                    ObParquetFileMeta &meta)
{
  int ret = OB_SUCCESS;
  ObParquetThriftReader reader;
  reader.reset(buf, len);
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t field_id = 0;
    int8_t type = 0;
    if (OB_FAIL(reader.read_field(field_id, type))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (2 == field_id) {
      int8_t elem_type = 0;
      int64_t element_count = 0;
      if (OB_FAIL(reader.read_list_header(elem_type, element_count))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < element_count; ++i) {
        ObString name;
        int32_t physical_type = -1;
        int32_t repetition = PARQUET_REP_REQUIRED;
        int32_t num_children = 0;
        int32_t converted_type = -1;
        if (OB_FAIL(parse_schema_element(reader, name, physical_type, repetition,
                                         num_children, converted_type))) {
        } else if (PARQUET_REP_REPEATED == repetition) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "repeated parquet columns");
        } else if (num_children > 0 || physical_type < 0) {
          // The root, or an intermediate group: only the leaves are exposed.
        } else {
          ObParquetLeaf leaf;
          if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, name, leaf.name_))) {
          } else {
            leaf.physical_type_ = physical_type;
            leaf.converted_type_ = converted_type;
            leaf.nullable_ = (PARQUET_REP_OPTIONAL == repetition);
            // A flat schema has no repetition levels, so the definition level
            // only says whether the value is present.
            leaf.max_def_level_ = leaf.nullable_ ? 1 : 0;
            if (OB_FAIL(meta.leaves_.push_back(leaf))) {
            }
          }
        }
      }
    } else if (3 == field_id) {
      ret = reader.read_i64(meta.num_rows_);
    } else if (4 == field_id) {
      int8_t elem_type = 0;
      int64_t row_group_count = 0;
      if (OB_FAIL(reader.read_list_header(elem_type, row_group_count))) {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_group_count; ++i) {
        ObParquetRowGroupMeta *row_group = NULL;
        if (OB_FAIL(parse_row_group(reader, alloc, row_group))) {
        } else if (OB_FAIL(meta.row_groups_.push_back(row_group))) {
        }
      }
    } else {
      ret = reader.skip(type);
    }
  }
  return ret;
}

int read_file_range(ObFileReader &file, const int64_t offset, const int64_t len,
                    char *buf)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  if (OB_FAIL(file.pread(buf, len, offset, read_size))) {
  } else if (read_size != len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("short read of the parquet file", K(ret), K(offset), K(len), K(read_size));
  }
  return ret;
}

int64_t parquet_leaf_col_type(const ObParquetLeaf &leaf)
{
  int64_t col_type = FILE_SQL_COL_VARCHAR;
  switch (leaf.physical_type_) {
    case PARQUET_BOOLEAN:
      col_type = FILE_SQL_COL_BOOLEAN;
      break;
    case PARQUET_INT32:
      col_type = (PARQUET_CONVERTED_DATE == leaf.converted_type_)
                   ? FILE_SQL_COL_DATE : FILE_SQL_COL_BIGINT;
      break;
    case PARQUET_INT64:
      col_type = (PARQUET_CONVERTED_TIMESTAMP_MILLIS == leaf.converted_type_ ||
                  PARQUET_CONVERTED_TIMESTAMP_MICROS == leaf.converted_type_)
                   ? FILE_SQL_COL_DATETIME : FILE_SQL_COL_BIGINT;
      break;
    case PARQUET_FLOAT:
    case PARQUET_DOUBLE:
      col_type = FILE_SQL_COL_DOUBLE;
      break;
    default:
      col_type = FILE_SQL_COL_VARCHAR;
      break;
  }
  return col_type;
}

int load_parquet_footer(ObIAllocator &alloc, const ObString &path,
                        ObFileReader &file, bool &file_opened,
                        ObParquetFileMeta &meta)
{
  int ret = OB_SUCCESS;
  int64_t file_size = 0;
  char footer_len_buf[PARQUET_FOOTER_LEN];
  ObCStringHelper helper;
  struct stat st;
  if (OB_FAIL(file.open(path, false))) {
    LOG_WARN("failed to open the parquet file", K(ret), K(path));
  } else {
    file_opened = true;
  }
  if (OB_FAIL(ret)) {
  } else if (0 != ::stat(helper.convert(path), &st)) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("failed to stat the parquet file", K(ret), K(path));
  } else if (FALSE_IT(file_size = st.st_size)) {
  } else if (file_size < PARQUET_MAGIC_LEN + PARQUET_FOOTER_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file is too small to be a parquet file", K(ret), K(file_size));
  } else if (OB_FAIL(read_file_range(file, file_size - PARQUET_FOOTER_LEN,
                                     PARQUET_FOOTER_LEN, footer_len_buf))) {
  } else {
    char magic[PARQUET_MAGIC_LEN];
    // The file ends with [metadata][4 byte metadata length]["PAR1"].
    const int64_t meta_len = static_cast<int64_t>(read_le(footer_len_buf, 4));
    const int64_t meta_offset = file_size - PARQUET_FOOTER_LEN - meta_len;
    if (0 != strncmp(footer_len_buf + 4, "PAR1", PARQUET_MAGIC_LEN) || meta_len <= 0 ||
        meta_offset < PARQUET_MAGIC_LEN) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not a parquet file", K(ret), K(file_size), K(meta_len));
    } else if (OB_FAIL(read_file_range(file, 0, PARQUET_MAGIC_LEN, magic))) {
    } else if (0 != strncmp(magic, "PAR1", PARQUET_MAGIC_LEN)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bad parquet magic", K(ret));
    } else {
      char *meta_buf = static_cast<char *>(alloc.alloc(meta_len));
      if (OB_ISNULL(meta_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(read_file_range(file, meta_offset, meta_len, meta_buf))) {
      } else if (OB_FAIL(parse_file_meta(alloc, meta_buf, meta_len, meta))) {
        LOG_WARN("failed to parse the parquet footer", K(ret));
      }
    }
  }
  return ret;
}

// ---- page decoding --------------------------------------------------------
// Expands an RLE / bit-packing hybrid run stream into `count` integers.
int decode_rle_hybrid(const char *buf, const int64_t len, const int bit_width,
                      const int64_t count, int32_t *values)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t produced = 0;
  while (produced < count) {
    values[produced++] = 0;
  }
  produced = 0;
  if (0 == bit_width) {
    // A width of zero means every value is zero.
    produced = count;
  }
  while (OB_SUCC(ret) && pos < len && produced < count) {
    uint64_t header = 0;
    int shift = 0;
    bool got_header = false;
    while (OB_SUCC(ret) && !got_header) {
      if (pos >= len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const unsigned char byte = static_cast<unsigned char>(buf[pos++]);
        header |= (static_cast<uint64_t>(byte) & 0x7f) << shift;
        if (0 == (byte & 0x80)) {
          got_header = true;
        } else {
          shift += 7;
          if (shift > 63) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 == (header & 1)) {
      // RLE run: one value repeated, stored in ceil(width / 8) bytes.
      const int64_t run = static_cast<int64_t>(header >> 1);
      const int byte_width = (bit_width + 7) / 8;
      if (pos + byte_width > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const int32_t value = static_cast<int32_t>(read_le(buf + pos, byte_width));
        pos += byte_width;
        for (int64_t i = 0; i < run && produced < count; ++i) {
          values[produced++] = value;
        }
      }
    } else {
      // Bit-packed run: groups of eight values at `bit_width` bits each.
      const int64_t groups = static_cast<int64_t>(header >> 1);
      const int64_t run = groups * 8;
      const int64_t bytes = groups * bit_width;
      if (pos + bytes > len) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        for (int64_t v = 0; v < run && produced < count; ++v) {
          int32_t value = 0;
          const int64_t bit_base = v * bit_width;
          for (int b = 0; b < bit_width; ++b) {
            const int64_t bit = bit_base + b;
            const int64_t byte_index = pos + (bit >> 3);
            if (buf[byte_index] & (1 << (bit & 7))) {
              value |= (1 << b);
            }
          }
          values[produced++] = value;
        }
        pos += bytes;
      }
    }
  }
  return ret;
}

// Decodes the PLAIN encoding of `count` values of one physical type.
int decode_plain(const char *buf, const int64_t len, const int32_t physical_type,
                 const int64_t count, ObIAllocator &alloc, ObParquetCell *cells)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObParquetCell &cell = cells[i];
    switch (physical_type) {
      case PARQUET_BOOLEAN: {
        const int64_t byte_index = i >> 3;
        if (byte_index >= len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          cell.int_value_ = (static_cast<unsigned char>(buf[byte_index]) >> (i & 7)) & 1;
        }
        break;
      }
      case PARQUET_INT32: {
        if (pos + 4 > len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          cell.int_value_ = static_cast<int32_t>(read_le(buf + pos, 4));
          pos += 4;
        }
        break;
      }
      case PARQUET_INT64: {
        if (pos + 8 > len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          cell.int_value_ = read_le(buf + pos, 8);
          pos += 8;
        }
        break;
      }
      case PARQUET_FLOAT: {
        if (pos + 4 > len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const int32_t bits = static_cast<int32_t>(read_le(buf + pos, 4));
          float value = 0;
          MEMCPY(&value, &bits, sizeof(float));
          cell.double_value_ = value;
          pos += 4;
        }
        break;
      }
      case PARQUET_DOUBLE: {
        if (pos + 8 > len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const int64_t bits = read_le(buf + pos, 8);
          MEMCPY(&cell.double_value_, &bits, sizeof(double));
          pos += 8;
        }
        break;
      }
      case PARQUET_BYTE_ARRAY: {
        if (pos + 4 > len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const int32_t value_len = static_cast<int32_t>(read_le(buf + pos, 4));
          pos += 4;
          if (value_len < 0 || pos + value_len > len) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(
                       alloc, ObString(value_len, buf + pos), cell.str_))) {
          } else {
            pos += value_len;
          }
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "this parquet physical type");
        break;
    }
  }
  return ret;
}

struct ObParquetPageHeader
{
  ObParquetPageHeader()
    : type_(PARQUET_PAGE_DATA), uncompressed_size_(0), compressed_size_(0),
      num_values_(0), encoding_(PARQUET_ENC_PLAIN), def_level_len_(0),
      rep_level_len_(0), dict_num_values_(0), is_compressed_(true),
      header_len_(0) {}
  int32_t type_;
  int64_t uncompressed_size_;
  int64_t compressed_size_;
  int64_t num_values_;
  int32_t encoding_;
  int64_t def_level_len_;
  int64_t rep_level_len_;
  int64_t dict_num_values_;
  bool is_compressed_;
  int64_t header_len_;
};

int parse_page_header(const char *buf, const int64_t len, ObParquetPageHeader &header)
{
  int ret = OB_SUCCESS;
  ObParquetThriftReader reader;
  reader.reset(buf, len);
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t field_id = 0;
    int8_t type = 0;
    if (OB_FAIL(reader.read_field(field_id, type))) {
    } else if (THRIFT_STOP == type) {
      done = true;
    } else if (1 == field_id) {
      ret = reader.read_i32(header.type_);
    } else if (2 == field_id) {
      int32_t uncompressed = 0;
      if (OB_FAIL(reader.read_i32(uncompressed))) {
      } else {
        header.uncompressed_size_ = uncompressed;
      }
    } else if (3 == field_id) {
      int32_t compressed = 0;
      if (OB_FAIL(reader.read_i32(compressed))) {
      } else {
        header.compressed_size_ = compressed;
      }
    } else if (5 == field_id || 7 == field_id || 8 == field_id) {
      // DataPageHeader, DictionaryPageHeader and DataPageHeaderV2 share most
      // field numbers, which is why they are read in one pass.
      const int32_t saved_page_base = reader.begin_struct();
      bool page_done = false;
      while (OB_SUCC(ret) && !page_done) {
        int32_t page_field = 0;
        int8_t page_type = 0;
        if (OB_FAIL(reader.read_field(page_field, page_type))) {
        } else if (THRIFT_STOP == page_type) {
          page_done = true;
        } else if (1 == page_field) {
          int32_t num_values = 0;
          if (OB_FAIL(reader.read_i32(num_values))) {
          } else if (5 == field_id) {
            header.num_values_ = num_values;
          } else {
            header.dict_num_values_ = num_values;
            header.num_values_ = num_values;
          }
        } else if (2 == page_field && 8 != field_id) {
          ret = reader.read_i32(header.encoding_);
        } else if (4 == page_field) {
          ret = reader.read_i32(header.encoding_);
        } else if (5 == page_field && 8 == field_id) {
          int32_t def_len = 0;
          if (OB_FAIL(reader.read_i32(def_len))) {
          } else {
            header.def_level_len_ = def_len;
          }
        } else if (6 == page_field && 8 == field_id) {
          int32_t rep_len = 0;
          if (OB_FAIL(reader.read_i32(rep_len))) {
          } else {
            header.rep_level_len_ = rep_len;
          }
        } else if (7 == page_field && 8 == field_id) {
          header.is_compressed_ = (THRIFT_BOOL_TRUE == page_type);
        } else {
          ret = reader.skip(page_type);
        }
      }
      reader.end_struct(saved_page_base);
    } else {
      ret = reader.skip(type);
    }
  }
  if (OB_SUCC(ret)) {
    header.header_len_ = reader.pos();
  }
  return ret;
}

int ensure_buf(char *&buf, int64_t &cap, const int64_t need)
{
  int ret = OB_SUCCESS;
  if (cap < need) {
    if (NULL != buf) {
      ob_free(buf);
      buf = NULL;
    }
    if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(need, "ParquetBuf")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      cap = 0;
    } else {
      cap = need;
    }
  }
  return ret;
}

// Decompresses a page payload according to the chunk's codec.
int decompress_payload(const int32_t codec, const char *src, const int64_t src_len,
                       const int64_t uncompressed_len, char *&buf, int64_t &buf_cap,
                       const char *&out, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  out = src;
  out_len = src_len;
  if (PARQUET_CODEC_UNCOMPRESSED == codec) {
  } else if (PARQUET_CODEC_SNAPPY == codec) {
    if (OB_FAIL(ensure_buf(buf, buf_cap, uncompressed_len))) {
    } else if (OB_FAIL(snappy_decompress(src, src_len, buf, buf_cap, out_len))) {
      LOG_WARN("failed to decompress a parquet page", K(ret), K(src_len), K(uncompressed_len));
    } else {
      out = buf;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "this parquet compression codec");
  }
  return ret;
}

} // namespace

// ---- the reader -----------------------------------------------------------
struct ObParquetColumnData
{
  ObParquetColumnData() : cells_(), decoded_(false) {}
  ObSEArray<ObParquetCell, 64, ModulePageAllocator, true> cells_;
  bool decoded_;
};

struct ObFileSqlParquetReader::Impl
{
  Impl()
    : alloc_(NULL), def_(NULL), file_opened_(false), meta_loaded_(false),
      projection_(), columns_(), row_group_idx_(-1), row_idx_(0),
      row_group_rows_(0), header_buf_(NULL), header_buf_cap_(0),
      page_buf_(NULL), page_buf_cap_(0), work_buf_(NULL), work_buf_cap_(0) {}
  ~Impl() { close(); }

  void close()
  {
    if (file_opened_) {
      file_.close();
      file_opened_ = false;
    }
    if (NULL != header_buf_) {
      ob_free(header_buf_);
      header_buf_ = NULL;
      header_buf_cap_ = 0;
    }
    if (NULL != page_buf_) {
      ob_free(page_buf_);
      page_buf_ = NULL;
      page_buf_cap_ = 0;
    }
    if (NULL != work_buf_) {
      ob_free(work_buf_);
      work_buf_ = NULL;
      work_buf_cap_ = 0;
    }
    meta_loaded_ = false;
    row_group_idx_ = -1;
    row_idx_ = 0;
    row_group_rows_ = 0;
    columns_.reset();
  }

  bool is_projected(const int64_t column_index) const
  {
    bool projected = projection_.empty();
    for (int64_t i = 0; !projected && i < projection_.count(); ++i) {
      projected = (projection_.at(i) == column_index);
    }
    return projected;
  }

  int load_meta();
  int decode_row_group();
  int decode_column(ObParquetColumnData &column, const ObParquetLeaf &leaf,
                    const ObParquetChunkMeta &chunk, const int64_t row_count);
  int decode_page_values(const ObParquetLeaf &leaf, const ObParquetChunkMeta &chunk,
                         const ObParquetPageHeader &header, const char *payload,
                         const int64_t page_payload_len,
                         const ObIArray<ObParquetCell> &dictionary,
                         ObIArray<ObParquetCell> &page_cells);
  int render_value(const ObParquetCell &value, const ObParquetLeaf &leaf,
                   ObIAllocator &alloc, ObString &text);

  ObIAllocator *alloc_;
  const ObFileSqlTableDef *def_;
  ObFileReader file_;
  bool file_opened_;
  bool meta_loaded_;
  ObParquetFileMeta meta_;
  ObSEArray<int64_t, 8, ModulePageAllocator, true> projection_;
  ObSEArray<ObParquetColumnData *, 8, ModulePageAllocator, true> columns_;
  int64_t row_group_idx_;
  int64_t row_idx_;
  int64_t row_group_rows_;
  char *header_buf_;
  int64_t header_buf_cap_;
  char *page_buf_;
  int64_t page_buf_cap_;
  char *work_buf_;
  int64_t work_buf_cap_;
};

int ObFileSqlParquetReader::Impl::load_meta()
{
  int ret = OB_SUCCESS;
  if (!meta_loaded_) {
    if (OB_FAIL(load_parquet_footer(*alloc_, def_->path_, file_, file_opened_, meta_))) {
    } else if (meta_.leaves_.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parquet file without columns", K(ret));
    } else {
      meta_loaded_ = true;
    }
  }
  return ret;
}

// Decodes the values of one page: definition levels, then the values.
int ObFileSqlParquetReader::Impl::decode_page_values(
    const ObParquetLeaf &leaf, const ObParquetChunkMeta &chunk,
    const ObParquetPageHeader &header, const char *payload,
    const int64_t page_payload_len, const ObIArray<ObParquetCell> &dictionary,
    ObIArray<ObParquetCell> &page_cells)
{
  int ret = OB_SUCCESS;
  const int64_t page_values = header.num_values_;
  const int def_bit_width = bit_width_of(leaf.max_def_level_);
  int64_t def_len = 0;
  int64_t rep_len = 0;
  const char *level_src = NULL;
  const char *value_src = NULL;
  int64_t value_src_len = 0;
  if (PARQUET_PAGE_DATA_V2 == header.type_) {
    rep_len = header.rep_level_len_;
    def_len = header.def_level_len_;
    if (rep_len + def_len > page_payload_len) {
      ret = OB_ERR_UNEXPECTED;
    } else if (leaf.max_def_level_ > 0) {
      level_src = payload + rep_len;
    }
    if (OB_FAIL(ret)) {
    } else {
      const int64_t value_compressed_len = page_payload_len - rep_len - def_len;
      const int64_t value_uncompressed_len =
          header.uncompressed_size_ - rep_len - def_len;
      if (!header.is_compressed_) {
        value_src = payload + rep_len + def_len;
        value_src_len = value_compressed_len;
      } else if (OB_FAIL(decompress_payload(chunk.codec_, payload + rep_len + def_len,
                                            value_compressed_len, value_uncompressed_len,
                                            work_buf_, work_buf_cap_,
                                            value_src, value_src_len))) {
      }
    }
  } else {
    // Version 1 page: the whole payload may be compressed, and a level section
    // encoded with RLE carries its own four byte length prefix.
    const char *plain_payload = NULL;
    int64_t plain_len = 0;
    if (OB_FAIL(decompress_payload(chunk.codec_, payload, page_payload_len,
                                   header.uncompressed_size_, work_buf_, work_buf_cap_,
                                   plain_payload, plain_len))) {
    }
    if (OB_FAIL(ret)) {
    } else if (leaf.max_def_level_ > 0) {
      if (plain_len < 4) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        def_len = static_cast<int32_t>(read_le(plain_payload, 4));
        if (4 + def_len > plain_len) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          level_src = plain_payload + 4;
          value_src = plain_payload + 4 + def_len;
          value_src_len = plain_len - 4 - def_len;
        }
      }
    } else {
      value_src = plain_payload;
      value_src_len = plain_len;
    }
  }
  UNUSED(rep_len);
  if (OB_FAIL(ret)) {
  } else {
    ObSEArray<int32_t, 64, ModulePageAllocator, true> levels;
    if (OB_FAIL(levels.prepare_allocate(page_values))) {
    } else if (leaf.max_def_level_ <= 0) {
      for (int64_t i = 0; i < page_values; ++i) {
        levels.at(i) = 0;
      }
    } else if (NULL != level_src) {
      if (OB_FAIL(decode_rle_hybrid(level_src, def_len, def_bit_width,
                                    page_values, &levels.at(0)))) {
      }
    } else {
      // A required column has no levels.
      for (int64_t i = 0; i < page_values; ++i) {
        levels.at(i) = leaf.max_def_level_;
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      int64_t non_null = 0;
      for (int64_t i = 0; i < page_values; ++i) {
        if (levels.at(i) >= leaf.max_def_level_) {
          ++non_null;
        }
      }
      ObSEArray<ObParquetCell, 64, ModulePageAllocator, true> values;
      if (OB_FAIL(values.prepare_allocate(non_null))) {
      } else if (non_null <= 0) {
        // A page of nulls needs no value decoding.
      } else if (PARQUET_ENC_PLAIN == header.encoding_) {
        if (OB_FAIL(decode_plain(value_src, value_src_len, leaf.physical_type_,
                                 non_null, *alloc_, &values.at(0)))) {
        }
      } else if (PARQUET_ENC_RLE_DICTIONARY == header.encoding_ ||
                 PARQUET_ENC_PLAIN_DICTIONARY == header.encoding_) {
        if (dictionary.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dictionary encoded page without a dictionary page", K(ret));
        } else if (value_src_len < 1) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          // The first byte is the bit width of the indices.
          const int index_width = static_cast<unsigned char>(value_src[0]);
          ObSEArray<int32_t, 64, ModulePageAllocator, true> indices;
          if (OB_FAIL(indices.prepare_allocate(non_null))) {
          } else if (OB_FAIL(decode_rle_hybrid(value_src + 1, value_src_len - 1,
                                               index_width, non_null, &indices.at(0)))) {
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < non_null; ++i) {
              const int32_t index = indices.at(i);
              if (index < 0 || index >= dictionary.count()) {
                ret = OB_ERR_UNEXPECTED;
              } else {
                values.at(i) = dictionary.at(index);
              }
            }
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "this parquet value encoding");
      }
      if (OB_FAIL(ret)) {
      } else {
        int64_t value_index = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < page_values; ++i) {
          ObParquetCell cell;
          if (levels.at(i) >= leaf.max_def_level_) {
            if (value_index >= values.count()) {
              ret = OB_ERR_UNEXPECTED;
            } else {
              cell = values.at(value_index++);
              cell.is_null_ = false;
            }
          } else {
            cell.is_null_ = true;
          }
          if (OB_SUCC(ret) && OB_FAIL(page_cells.push_back(cell))) {
          }
        }
      }
    }
  }
  return ret;
}

// Decodes one column chunk of the current row group.
int ObFileSqlParquetReader::Impl::decode_column(ObParquetColumnData &column,
                                                const ObParquetLeaf &leaf,
                                                const ObParquetChunkMeta &chunk,
                                                const int64_t row_count)
{
  int ret = OB_SUCCESS;
  int64_t offset = chunk.data_page_offset_;
  if (chunk.dict_page_offset_ > 0 && (offset < 0 || chunk.dict_page_offset_ < offset)) {
    offset = chunk.dict_page_offset_;
  }
  ObSEArray<ObParquetCell, 64, ModulePageAllocator, true> dictionary;
  int64_t values_seen = 0;
  while (OB_SUCC(ret) && values_seen < chunk.num_values_ && offset >= 0) {
    int64_t read_size = 0;
    if (OB_FAIL(ensure_buf(header_buf_, header_buf_cap_, PARQUET_PAGE_HEADER_READ_LEN))) {
    } else if (OB_FAIL(file_.pread(header_buf_, PARQUET_PAGE_HEADER_READ_LEN, offset,
                                   read_size))) {
    } else if (0 == read_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected end of the parquet file", K(ret), K(offset));
    } else {
      ObParquetPageHeader header;
      if (OB_FAIL(parse_page_header(header_buf_, read_size, header))) {
      } else if (header.compressed_size_ < 0 || header.uncompressed_size_ < 0 ||
                 header.header_len_ + header.compressed_size_ > INT32_MAX) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(ensure_buf(page_buf_, page_buf_cap_,
                                    header.header_len_ + header.compressed_size_))) {
      } else if (OB_FAIL(read_file_range(file_, offset,
                                         header.header_len_ + header.compressed_size_,
                                         page_buf_))) {
      } else {
        const char *payload = page_buf_ + header.header_len_;
        offset += header.header_len_ + header.compressed_size_;
        if (PARQUET_PAGE_DICTIONARY == header.type_) {
          // The whole payload is one PLAIN array.
          const char *dict_src = NULL;
          int64_t dict_len = 0;
          if (OB_FAIL(decompress_payload(chunk.codec_, payload, header.compressed_size_,
                                         header.uncompressed_size_, work_buf_,
                                         work_buf_cap_, dict_src, dict_len))) {
          } else if (OB_FAIL(dictionary.prepare_allocate(header.dict_num_values_))) {
          } else if (header.dict_num_values_ <= 0) {
            // An empty dictionary is legal.
          } else if (OB_FAIL(decode_plain(dict_src, dict_len, leaf.physical_type_,
                                          header.dict_num_values_, *alloc_,
                                          &dictionary.at(0)))) {
          }
        } else if (PARQUET_PAGE_DATA == header.type_ ||
                   PARQUET_PAGE_DATA_V2 == header.type_) {
          if (OB_FAIL(decode_page_values(leaf, chunk, header, payload,
                                         header.compressed_size_, dictionary,
                                         column.cells_))) {
          } else {
            values_seen += header.num_values_;
          }
        } else {
          // Index pages carry no values.
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    column.decoded_ = true;
    if (column.cells_.count() < row_count) {
      // Defensive: pad with NULL so a truncated chunk cannot shift whole rows.
      ObParquetCell null_cell;
      while (OB_SUCC(ret) && column.cells_.count() < row_count) {
        ret = column.cells_.push_back(null_cell);
      }
    }
  }
  return ret;
}

int ObFileSqlParquetReader::Impl::decode_row_group()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_group_idx_ < 0 || row_group_idx_ >= meta_.row_groups_.count())) {
    ret = OB_ITER_END;
  } else {
    const ObParquetRowGroupMeta *row_group = meta_.row_groups_.at(row_group_idx_);
    if (OB_ISNULL(row_group) ||
        OB_UNLIKELY(row_group->columns_.count() != meta_.leaves_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parquet row group does not match the schema", K(ret));
    } else {
      row_group_rows_ = row_group->num_rows_;
      row_idx_ = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
        if (OB_ISNULL(columns_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          columns_.at(i)->cells_.reuse();
          columns_.at(i)->decoded_ = false;
        }
      }
      // Column pruning: only the projected columns are decoded.
      for (int64_t i = 0; OB_SUCC(ret) && i < meta_.leaves_.count(); ++i) {
        if (!is_projected(i)) {
          continue;
        } else if (OB_FAIL(decode_column(*columns_.at(i), meta_.leaves_.at(i),
                                         row_group->columns_.at(i), row_group_rows_))) {
          LOG_WARN("failed to decode a parquet column", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

// Renders a decoded value as the text the scan operator expects.
int ObFileSqlParquetReader::Impl::render_value(const ObParquetCell &value,
                                               const ObParquetLeaf &leaf,
                                               ObIAllocator &alloc,
                                               ObString &text)
{
  int ret = OB_SUCCESS;
  if (PARQUET_BYTE_ARRAY == leaf.physical_type_) {
    if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, value.str_, text))) {
    }
  } else if (PARQUET_BOOLEAN == leaf.physical_type_) {
    const char *raw = value.int_value_ ? "true" : "false";
    if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, ObString::make_string(raw),
                                                    text))) {
    }
  } else if (PARQUET_FLOAT == leaf.physical_type_ ||
             PARQUET_DOUBLE == leaf.physical_type_) {
    char buf[64];
    const int32_t len = snprintf(buf, sizeof(buf), "%.17g", value.double_value_);
    if (len <= 0 || OB_FAIL(ObFileSqlTableDef::deep_copy_string(
                    alloc, ObString(len, buf), text))) {
    }
  } else if (PARQUET_CONVERTED_DATE == leaf.converted_type_) {
    char buf[64];
    int64_t pos = 0;
    if (OB_FAIL(ObTimeConverter::date_to_str(static_cast<int32_t>(value.int_value_),
                                             buf, sizeof(buf), pos))) {
    } else if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(
                 alloc, ObString(static_cast<int32_t>(pos), buf), text))) {
    }
  } else if (PARQUET_CONVERTED_TIMESTAMP_MILLIS == leaf.converted_type_ ||
             PARQUET_CONVERTED_TIMESTAMP_MICROS == leaf.converted_type_) {
    // Parquet counts milliseconds or microseconds since the epoch, while the
    // engine uses microseconds.
    const int64_t micros = (PARQUET_CONVERTED_TIMESTAMP_MILLIS == leaf.converted_type_)
                             ? value.int_value_ * 1000 : value.int_value_;
    char buf[64];
    int64_t pos = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_str(micros, NULL, 0, buf, sizeof(buf), pos))) {
    } else if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(
                 alloc, ObString(static_cast<int32_t>(pos), buf), text))) {
    }
  } else {
    char buf[32];
    const int32_t len = snprintf(buf, sizeof(buf), "%ld", value.int_value_);
    if (len <= 0 || OB_FAIL(ObFileSqlTableDef::deep_copy_string(
                    alloc, ObString(len, buf), text))) {
    }
  }
  return ret;
}

int read_parquet_meta(ObIAllocator &alloc, const ObString &path,
                      ObFileSqlParquetMeta &meta)
{
  int ret = OB_SUCCESS;
  ObFileReader file;
  bool file_opened = false;
  ObParquetFileMeta file_meta;
  if (OB_FAIL(load_parquet_footer(alloc, path, file, file_opened, file_meta))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_meta.leaves_.count(); ++i) {
      const ObParquetLeaf &leaf = file_meta.leaves_.at(i);
      ObFileSqlParquetColumn column;
      if (OB_FAIL(ObFileSqlTableDef::deep_copy_string(alloc, leaf.name_, column.name_))) {
      } else {
        column.col_type_ = parquet_leaf_col_type(leaf);
        column.physical_type_ = leaf.physical_type_;
        column.nullable_ = leaf.nullable_;
        if (OB_FAIL(meta.columns_.push_back(column))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      meta.row_cnt_ = file_meta.num_rows_;
    }
  }
  if (file_opened) {
    file.close();
  }
  return ret;
}

ObFileSqlParquetReader::ObFileSqlParquetReader() : impl_(NULL) {}
ObFileSqlParquetReader::~ObFileSqlParquetReader() { close(); }

int ObFileSqlParquetReader::init(const ObFileSqlTableDef &def, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = alloc.alloc(sizeof(Impl)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (FALSE_IT(impl_ = new (buf) Impl())) {
  } else {
    impl_->alloc_ = &alloc;
    impl_->def_ = &def;
    column_count_ = def.column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < def.column_count(); ++i) {
      void *col_buf = NULL;
      ObParquetColumnData *column = NULL;
      if (OB_ISNULL(col_buf = alloc.alloc(sizeof(ObParquetColumnData)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (FALSE_IT(column = new (col_buf) ObParquetColumnData())) {
      } else if (OB_FAIL(impl_->columns_.push_back(column))) {
      }
    }
    // The projection is optional; an empty list means every column is decoded.
    for (int64_t i = 0; OB_SUCC(ret) && i < def.projection_.count(); ++i) {
      if (OB_FAIL(impl_->projection_.push_back(def.projection_.at(i)))) {
      }
    }
  }
  return ret;
}

int ObFileSqlParquetReader::open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(impl_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(impl_->load_meta())) {
  } else {
    impl_->row_group_idx_ = 0;
    impl_->row_idx_ = 0;
    impl_->row_group_rows_ = 0;
  }
  return ret;
}

void ObFileSqlParquetReader::close()
{
  if (NULL != impl_) {
    impl_->close();
    impl_->~Impl();
    impl_ = NULL;
  }
}

int ObFileSqlParquetReader::get_next_row(ObIAllocator &alloc, ObIArray<ObFileSqlCell> &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  if (OB_ISNULL(impl_) || OB_ISNULL(impl_->def_)) {
    ret = OB_NOT_INIT;
  } else {
    while (OB_SUCC(ret) && impl_->row_idx_ >= impl_->row_group_rows_) {
      if (impl_->row_group_idx_ >= impl_->meta_.row_groups_.count()) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(impl_->decode_row_group())) {
      } else {
        ++impl_->row_group_idx_;
        impl_->row_idx_ = 0;
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      const int64_t row_index = impl_->row_idx_;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_count_; ++i) {
        ObFileSqlCell cell;
        ObParquetColumnData *column = impl_->columns_.at(i);
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (!column->decoded_ || row_index >= column->cells_.count()) {
          // A pruned column is never read.
          cell.is_null_ = true;
        } else {
          const ObParquetCell &value = column->cells_.at(row_index);
          if (value.is_null_) {
            cell.is_null_ = true;
          } else {
            cell.is_null_ = false;
            if (OB_FAIL(impl_->render_value(value, impl_->meta_.leaves_.at(i),
                                            alloc, cell.str_))) {
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(row.push_back(cell))) {
        }
      }
      if (OB_SUCC(ret)) {
        ++impl_->row_idx_;
      }
    }
  }
  return ret;
}

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

} // namespace sql
} // namespace oceanbase
