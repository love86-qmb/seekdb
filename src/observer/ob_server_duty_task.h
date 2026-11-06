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
#include "lib/ob_define.h"
#include "lib/task/ob_timer.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"

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
//
// One pending task row joined with the metadata of its AI column.
struct ObAiColumnTaskRow
{
  ObAiColumnTaskRow()
    : table_id_(OB_INVALID_ID), column_id_(OB_INVALID_ID), request_id_(0), retry_count_(0),
      max_retries_(0)
  {
  }
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t request_id_;
  int64_t retry_count_;
  int64_t max_retries_;
  common::ObString row_key_;
  common::ObString rowkey_predicate_;
  common::ObString column_name_;
  common::ObString expr_text_;
  common::ObString table_name_;
  common::ObString database_name_;
};

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
  // The AI columns of one row that the same statement created. They are rendered and
  // written together, because one UPDATE per group is what keeps a reader from seeing
  // one column of the row filled while another one is still missing.
  int process_task_group(const common::ObIArray<const ObAiColumnTaskRow *> &group);
  // delete the task rows of %group, then write %results into the columns of the row
  int write_task_group(const common::ObIArray<const ObAiColumnTaskRow *> &group,
                       const common::ObIArray<common::ObString> &results);
  int delete_task_group(const common::ObIArray<const ObAiColumnTaskRow *> &group);
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
  // Schedule another attempt of %task. A task that already used up its retries is
  // dropped instead of being kept as FAILED, so a failed row leaves no task row behind:
  // the row keeps its NULL value and an explicit
  // "ALTER TABLE ... RETRY AI COLUMN ..." brings the task back.
  int retry_later(const ObAiColumnTaskRow &task, const int error_code);
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
