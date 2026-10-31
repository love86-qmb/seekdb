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

#ifndef OB_SERVER_DUTY_TASK_H
#define OB_SERVER_DUTY_TASK_H
#include <stdint.h>
#include "lib/task/ob_timer.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
namespace observer {

class ObServerDutyTask
    : private common::ObTimerTask
{
  static constexpr int64_t SCHEDULE_PERIOD = 10 * 1000L * 1000L;
public:
  int schedule(common::ObTimer &timer);
  ObServerDutyTask();
private:
  void runTimerTask() override;
  void update_runtime_settings();

private:
  // Apply context-memory limits from the runtime configuration.
  int update_ctx_memory_throttle();
  common::ObArenaAllocator allocator_;
};

class ObSqlMemoryTimerTask : private common::ObTimerTask
{
public:
  int schedule(common::ObTimer &timer);
private:
  void runTimerTask() override;
private:
  static constexpr int64_t SCHEDULE_PERIOD = 3 * 1000L * 1000L;
};

// Drives the asynchronous AI columns: it owns the pending task rows of
// __all_ai_column_task, asks the model of the AI column for a value and writes the
// result back into the user table.
class ObAiColumnServerWorker : private common::ObTimerTask
{
public:
  ObAiColumnServerWorker();
  virtual ~ObAiColumnServerWorker() {}
  int schedule(common::ObTimer &timer);
private:
  void runTimerTask() override;
  // one round over the pending task rows
  int process_pending_tasks();
  int process_one_task(const int64_t table_id,
                       const int64_t column_id,
                       const common::ObString &row_key,
                       const common::ObString &rowkey_predicate,
                       const int64_t retry_count,
                       const int64_t max_retries,
                       const common::ObString &column_name,
                       const common::ObString &expr_text,
                       const common::ObString &table_name,
                       const common::ObString &database_name);
  // the typed system prompt that selects the answer format of the column
  int get_typed_prompt(const int64_t table_id,
                       const int64_t column_id,
                       common::ObString &typed_prompt);
  // render the user side value of a row with an internal "select <expr>" query
  int render_value(common::ObIAllocator &allocator,
                   const common::ObString &database_name,
                   const common::ObString &table_name,
                   const common::ObString &value_expr,
                   const common::ObString &rowkey_predicate,
                   common::ObString &value,
                   bool &found);
  int call_model(common::ObIAllocator &allocator,
                 const common::ObString &model_name,
                 const common::ObString &typed_prompt,
                 const common::ObString &content,
                 common::ObString &result);
  int delete_task(const int64_t table_id,
                  const int64_t column_id,
                  const common::ObString &row_key);
  int update_row(const common::ObString &database_name,
                 const common::ObString &table_name,
                 const common::ObString &column_name,
                 const common::ObString &rowkey_predicate,
                 const common::ObString &value);
  int retry_later(const int64_t table_id,
                  const int64_t column_id,
                  const common::ObString &row_key,
                  const int64_t retry_count,
                  const int64_t max_retries,
                  const int error_code);
  // log a failure, throttled: the log of this deployment keeps INFO lines only
  void report_error(const int error_code);
private:
  static constexpr int64_t SCHEDULE_PERIOD = 100 * 1000L;
  static constexpr int64_t MAX_TASK_PER_ROUND = 8;
  common::ObArenaAllocator allocator_;
  bool inited_;
  int64_t last_error_ts_;
};

// process wide instance of the AI column worker, owned by the server timer
ObAiColumnServerWorker &ai_column_server_worker();

}  // observer
}  // oceanbase

#endif /* OB_SERVER_DUTY_TASK_H */
