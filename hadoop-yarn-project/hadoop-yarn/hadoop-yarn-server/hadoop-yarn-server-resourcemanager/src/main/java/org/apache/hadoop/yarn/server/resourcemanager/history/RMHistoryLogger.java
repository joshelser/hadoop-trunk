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
package org.apache.hadoop.yarn.server.resourcemanager.history;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;

@Private
@Unstable
public class RMHistoryLogger extends RMHistoryStore {

  private static final Log LOG = LogFactory.getLog(RMHistoryLogger.class);
  
  @Override
  public void applicationSubmitted(RMHistoryAppSubmittedEvent event) {
    LOG.info("Application submitted: " + event.getApplicationId());
  }

  @Override
  public void applicationLaunched(RMHistoryAppLaunchedEvent event) {
    LOG.info("Application launched: " + event.getApplicationId());
  }

  @Override
  public void applicationCompleted(RMHistoryAppCompletedEvent event) {
    LOG.info("Application completed: " + event.getApplicationId());
  }

  @Override
  protected void initInternal(Configuration conf) throws Exception {
    // noop
  }

  @Override
  protected void closeInternal() throws Exception {
    // noop
  }

}
