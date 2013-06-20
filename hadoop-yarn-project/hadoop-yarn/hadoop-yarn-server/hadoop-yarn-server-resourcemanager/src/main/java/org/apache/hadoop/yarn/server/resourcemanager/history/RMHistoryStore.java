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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

@Private
@Unstable
public abstract class RMHistoryStore {

  private static final String WORKFLOW_ADJACENCY_PREFIX_STRING =
      "mapreduce.workflow.adjacency.";
  private static final String WORKFLOW_ADJACENCY_PREFIX_PATTERN =
      "^mapreduce\\.workflow\\.adjacency\\..+";

  AsyncDispatcher dispatcher;

  public synchronized void init(Configuration conf) throws Exception{    
    // create async handler
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.register(RMHistoryEventType.class, 
                        new ForwardingEventHandler());
    dispatcher.start();
    
    initInternal(conf);
  }

  /**
   * Derived classes initialize themselves using this method.
   * The base class is initialized and the event dispatcher is ready to use at
   * this point
   */
  protected abstract void initInternal(Configuration conf) throws Exception;
  
  public synchronized void close() throws Exception {
    closeInternal();
    dispatcher.stop();
  }
  
  /**
   * Derived classes close themselves using this method.
   * The base class will be closed and the event dispatcher will be shutdown 
   * after this
   */
  protected abstract void closeInternal() throws Exception;
  
  public static String escapeString(String data) {
    return StringUtils.escapeString(
        data, StringUtils.ESCAPE_CHAR,new char[] {'"', '=', '.'});
  }

  /**
   * Application submission event.
   */
  @SuppressWarnings("unchecked")
  public void submitApplication(RMApp app) {
    Map<String, String> appEnv = 
        app.getApplicationSubmissionContext().getAMContainerSpec().getEnvironment();
    
    dispatcher.getEventHandler().handle(
        new RMHistoryAppSubmittedEvent(
            app.getApplicationId(), app.getName(), 
            app.getApplicationType(), 
            appEnv.get(YarnConfiguration.APPLICATION_INFO),
            app.getUser(), app.getQueue(),
            app.getSubmitTime(), 
            appEnv.get(YarnConfiguration.APPLICATION_WORKFLOW_CONTEXT)
            )
        );
  }
  
  /**
   * Application submission event.
   */
  protected abstract void applicationSubmitted(
      RMHistoryAppSubmittedEvent event);

  /**
   * Application launch event.
   */
  @SuppressWarnings("unchecked")
  public void launchApplication(RMApp app) {
    dispatcher.getEventHandler().handle(
        new RMHistoryAppLaunchedEvent(
            app.getApplicationId(), app.getStartTime()));
  }

  /**
   * Application launch event.
   */
  protected abstract void applicationLaunched(RMHistoryAppLaunchedEvent event);

  /**
   * Application completion event.
   */
  @SuppressWarnings("unchecked")
  public void completeApplication(RMApp app, RMAppState state) {
    dispatcher.getEventHandler().handle(
        new RMHistoryAppCompletedEvent(
            app.getApplicationId(), app.getFinishTime(), 
            state));
  }
  
  /**
   * Application completion event.
   */
  protected abstract void applicationCompleted(RMHistoryAppCompletedEvent event);
  
  
  private synchronized void handleStoreEvent(RMHistoryEvent event) {
    switch(event.getType()) {
      case APP_SUBMITTED:
        { 
          applicationSubmitted((RMHistoryAppSubmittedEvent) event);
        }
        break;
      case APP_LAUNCHED:
        {
          applicationLaunched((RMHistoryAppLaunchedEvent) event);
        }
        break;
      case APP_COMPLETED:
      {
        applicationCompleted((RMHistoryAppCompletedEvent) event);
      }
      break;
    }
  }
  
      
  /**
   * EventHandler implementation which forward events to the FSRMStateStore
   * This hides the EventHandle methods of the store from its public interface 
   */
  private final class ForwardingEventHandler 
                                  implements EventHandler<RMHistoryEvent> {
    
    @Override
    public void handle(RMHistoryEvent event) {
      handleStoreEvent(event);
    }
  }}
