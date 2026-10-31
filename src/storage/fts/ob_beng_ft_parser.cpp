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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_beng_ft_parser.h"
#include "data_plane/fts/ob_fts_struct.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
namespace
{

struct ObBEngAnalyzerCache
{
  ObBEngAnalyzerCache()
      : allocator_(ObMemAttr("BEngAnalyzer")), ctx_(), analyzer_(), cs_(nullptr),
        initialized_(false), in_use_(false)
  {}

  common::ObArenaAllocator allocator_;
  share::ObTextAnalysisCtx ctx_;
  share::ObEnglishTextAnalyzer analyzer_;
  const ObCharsetInfo *cs_;
  bool initialized_;
  bool in_use_;
};

thread_local ObBEngAnalyzerCache BENG_ANALYZER_CACHE;

int acquire_beng_analyzer(const ObCharsetInfo *cs,
                          share::ObEnglishTextAnalyzer &local_analyzer,
                          share::ObTextAnalysisCtx &local_ctx,
                          ObIAllocator &local_allocator,
                          share::ObEnglishTextAnalyzer *&analyzer,
                          bool &uses_shared)
{
  int ret = OB_SUCCESS;
  uses_shared = false;
  analyzer = nullptr;
  if (OB_ISNULL(cs)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!BENG_ANALYZER_CACHE.in_use_) {
    if (!BENG_ANALYZER_CACHE.initialized_ || BENG_ANALYZER_CACHE.cs_ != cs) {
      BENG_ANALYZER_CACHE.analyzer_.reset();
      BENG_ANALYZER_CACHE.allocator_.reset();
      BENG_ANALYZER_CACHE.ctx_.reset();
      BENG_ANALYZER_CACHE.ctx_.cs_ = cs;
      BENG_ANALYZER_CACHE.ctx_.filter_stopword_ = false;
      BENG_ANALYZER_CACHE.ctx_.need_grouping_ = false;
      if (OB_FAIL(BENG_ANALYZER_CACHE.analyzer_.init(BENG_ANALYZER_CACHE.ctx_,
                                                     BENG_ANALYZER_CACHE.allocator_))) {
        BENG_ANALYZER_CACHE.initialized_ = false;
      } else {
        BENG_ANALYZER_CACHE.cs_ = cs;
        BENG_ANALYZER_CACHE.initialized_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      BENG_ANALYZER_CACHE.in_use_ = true;
      analyzer = &BENG_ANALYZER_CACHE.analyzer_;
      uses_shared = true;
    }
  } else {
    local_ctx.cs_ = cs;
    local_ctx.filter_stopword_ = false;
    local_ctx.need_grouping_ = false;
    if (OB_FAIL(local_analyzer.init(local_ctx, local_allocator))) {
    } else {
      analyzer = &local_analyzer;
    }
  }
  return ret;
}

void release_beng_analyzer(const bool uses_shared)
{
  if (uses_shared) {
    BENG_ANALYZER_CACHE.in_use_ = false;
  }
}

} // namespace

int ObBEngFTParser::get_next_token(
    const char *&word,
    int64_t &word_len,
    int64_t &char_len,
    int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  ObDatum token;
  int64_t token_freq = 0;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(token_stream_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(token_stream_->get_next(token, token_freq))) {
    if (OB_ITER_END != ret) {
    }
  } else if (OB_ISNULL(token.ptr_) || OB_UNLIKELY(0 >= token.len_ || 0 >= token_freq)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // The token stream owns this buffer until the next get_next() call.  The
    // caller consumes/copies the token synchronously, as the whitespace and
    // ngram parsers already do, so another arena allocation and memcpy here is
    // redundant for every English token.
    word = token.ptr_;
    word_len = token.len_;
    char_len = token.len_;
    word_freq = token_freq;
    LOG_DEBUG("succeed to add word", K(ObString(word_len, word)), K(word_freq));
  }
  return ret;
}

int ObBEngFTParser::init(ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(UINT32_MAX < param->ft_length_)) {
    ret = OB_NOT_SUPPORTED;
  } else {
    doc_.set_string(param->fulltext_, param->ft_length_);
    if (OB_FAIL(acquire_beng_analyzer(param->cs_,
                                      local_english_analyzer_,
                                      analysis_ctx_,
                                      *param->allocator_,
                                      english_analyzer_,
                                      uses_shared_analyzer_))) {
    } else if (OB_FAIL(segment(doc_, token_stream_))) {
    } else if (OB_ISNULL(token_stream_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObBEngFTParser::segment(
    const common::ObDatum &doc,
    share::ObITokenStream *&token_stream)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(doc.ptr_) || OB_UNLIKELY(0 >= doc.len_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(UINT32_MAX < doc.len_)) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_ISNULL(english_analyzer_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(english_analyzer_->analyze(doc, token_stream))) {
  }
  return ret;
}

void ObBEngFTParser::reset()
{
  release_beng_analyzer(uses_shared_analyzer_);
  if (!uses_shared_analyzer_) {
    local_english_analyzer_.reset();
  }
  analysis_ctx_.reset();
  english_analyzer_ = nullptr;
  doc_.reset();
  token_stream_ = nullptr;
  uses_shared_analyzer_ = false;
  is_inited_ = false;
}

ObBasicEnglishFTParserDesc::ObBasicEnglishFTParserDesc() {}

int ObBasicEnglishFTParserDesc::segment(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  ObBEngFTParser *parser = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(parser = OB_NEWx(ObBEngFTParser, param->allocator_, *(param->allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(parser->init(param))) {
  } else {
    iter = parser;
  }

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObBEngFTParser, param->allocator_, parser);
  }

  return ret;
}

void ObBasicEnglishFTParserDesc::free_token_iter(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    abort_unless(nullptr != param);
    abort_unless(nullptr != param->allocator_);
    iter->~ObITokenIterator();
    param->allocator_->free(iter);
  }
}

int ObBasicEnglishFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_min_max_word();
  flag.set_stop_word();
  flag.set_groupby_word();
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
