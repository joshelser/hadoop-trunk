/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.rumen;

import org.apache.hadoop.mapreduce.TaskID;

/**
 * Event to record updates to a task
 *
 */
public class TaskUpdatedEvent implements HistoryEvent {
  private TaskID taskId;
  private long finishTime;

  /**
   * Create an event to record task updates
   * @param id Id of the task
   * @param finishTime Finish time of the task
   */
  public TaskUpdatedEvent(TaskID id, long finishTime) {
    this.taskId = id;
    this.finishTime = finishTime;
  }

  /** Get the task ID */
  public TaskID getTaskId() { return taskId; }
  /** Get the task finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the event type */
  public EventType getEventType() {
    return EventType.TASK_UPDATED;
  }
  @Override
  public String toString() {
    return getEventType() + ":" + taskId.getJobID() + ":" + 
      taskId + ":" + finishTime;
  }

}
