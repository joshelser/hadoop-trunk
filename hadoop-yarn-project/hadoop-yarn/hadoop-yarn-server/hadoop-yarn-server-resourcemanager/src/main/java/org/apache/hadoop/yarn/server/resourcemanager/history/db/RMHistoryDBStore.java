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
package org.apache.hadoop.yarn.server.resourcemanager.history.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.history.RMHistoryAppCompletedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.history.RMHistoryAppLaunchedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.history.RMHistoryAppSubmittedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.history.RMHistoryStore;
import org.apache.hadoop.yarn.util.WorkflowContext;
import org.apache.hadoop.yarn.util.WorkflowDag;
import org.apache.hadoop.yarn.util.WorkflowDag.WorkflowDagEntry;
import org.codehaus.jackson.map.ObjectMapper;

@Private
@Unstable
public class RMHistoryDBStore extends RMHistoryStore {

  private static final Log LOG = LogFactory.getLog(RMHistoryDBStore.class);
  
  private String driver;
  private String database;
  private String user;
  private String password;
  private Connection connection;
  
  private final String RM_HISTORY_DB_STORE_PREFIX = 
      YarnConfiguration.RM_HISTORY_PREFIX + "db.";  
      
  private final String RM_HISTORY_DB_STORE_DRIVER = 
      RM_HISTORY_DB_STORE_PREFIX + "driver";

  private final String RM_HISTORY_DB_STORE_DATABASE = 
      RM_HISTORY_DB_STORE_PREFIX + "database";

  private final String RM_HISTORY_DB_STORE_USER = 
      RM_HISTORY_DB_STORE_PREFIX + "user";

  private final String RM_HISTORY_DB_STORE_PASSWORD = 
      RM_HISTORY_DB_STORE_PREFIX + "password";
  
  private final String APPLICATION_TABLE = "application";
  /**
   *                      Table "application"
   *                Column       |  Type   | Modifiers
   *         --------------------+---------+-----------
   *          appid              | text    | not null
   *          workflowid         | text    |
   *          appname            | text    |
   *          workflowentityname | text    |
   *          username           | text    |
   *          queue              | text    |
   *          submittime         | bigint  |
   *          launchtime         | bigint  |
   *          finishtime         | bigint  |
   *          apptype            | text    |
   *          status             | text    |
   *          appInfo            | text    |
   *         
   *          Indexes:
   *          "application_pkey" PRIMARY KEY, btree (appid)
   *          Foreign-key constraints:
   *          "application_workflowid_fkey" FOREIGN KEY (workflowid) REFERENCES workflow(workflowid)
   */
  
  private final String WORKFLOW_TABLE = "workflow";
  /**  
   *                    Table "workflow"
   *             Column      |  Type   | Modifiers
   *       ------------------+---------+-----------
   *        workflowid       | text    | not null
   *        workflowname     | text    |
   *        parentworkflowid | text    |
   *        workflowcontext  | text    |
   *        workflowtags     | text    |
   *        username         | text    |
   *        starttime        | bigint  |
   *        lastupdatetime   | bigint  |
   *        numjobstotal     | integer |
   *        numjobscompleted | integer |
   *        inputbytes       | bigint  |
   *        outputbytes      | bigint  |
   *        duration         | bigint  |
   *       
   *        Indexes:
   *        "workflow_pkey" PRIMARY KEY, btree (workflowid)
   *        Foreign-key constraints:
   *        "workflow_parentworkflowid_fkey" FOREIGN KEY (parentworkflowid) REFERENCES workflow(workflowid)
   *        Referenced by:
   *        TABLE "application" CONSTRAINT "application_workflowid_fkey" FOREIGN KEY (workflowid) REFERENCES workflow(workflowid)
   *        TABLE "workflow" CONSTRAINT "workflow_parentworkflowid_fkey" FOREIGN KEY (parentworkflowid) REFERENCES workflow(workflowid)
   */
  
  
  PreparedStatement appSubmittedPS;
  PreparedStatement appLaunchedPS;
  PreparedStatement appCompletedPS;
  
  PreparedStatement workflowSelectPS;
  PreparedStatement workflowPS;
  PreparedStatement workflowUpdateTimePS;
  PreparedStatement workflowUpdateNumCompletedPS;
  
  @Override
  protected void initInternal(Configuration conf) throws Exception {
    
    driver = conf.get(RM_HISTORY_DB_STORE_DRIVER, "UNKNOWN");
    database = conf.get(RM_HISTORY_DB_STORE_DATABASE, "UNKNOWN");
    user = conf.get(RM_HISTORY_DB_STORE_USER, "");
    password = conf.get(RM_HISTORY_DB_STORE_PASSWORD, "");
    
    LOG.info("Connecting to '" + database + "' as '" + user + 
        "' with passwd '" + password + "' using '" + driver +"'");
    
    Class.forName(driver);

    this.connection = 
        DriverManager.getConnection(this.database, this.user, this.password);
    
    initializePreparedStatements();
  }

  private void initializePreparedStatements() throws SQLException {
    appSubmittedPS =
        connection.prepareStatement(
            "INSERT INTO " + 
                APPLICATION_TABLE + 
                " (" +
                "appId, " +
                "appName, " +
                "appType, " +
                "userName, " +
                "queue, " +
                "submitTime, " +
                "workflowId, " +
                "workflowEntityName, " +
                "appInfo " +
                ") " +
                "VALUES" +
                " (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            );
    
    workflowSelectPS =
        connection.prepareStatement(
            "SELECT workflowContext, workflowTags FROM " + WORKFLOW_TABLE + " where workflowId = ?"
            );

    workflowPS = 
        connection.prepareStatement(
            "INSERT INTO " +
                WORKFLOW_TABLE +
                " (" +
                "workflowId, " +
                "workflowName, " +
                "workflowContext, " +
                "userName, " +
                "startTime, " +
                "lastUpdateTime, " +
                "duration, " +
                "numJobsTotal, " +
                "numJobsCompleted, " +
                "workflowTags " +
                ") " +
                "VALUES" +
                " (?, ?, ?, ?, ?, ?, 0, ?, 0, ?)"
            );

    workflowUpdateTimePS =
        connection.prepareStatement(
            "UPDATE " +
                WORKFLOW_TABLE +
                " SET " +
                "workflowContext = ?, " +
                "numJobsTotal = ?, " +
                "lastUpdateTime = ?, " +
                "duration = ? - (SELECT startTime FROM " +
                WORKFLOW_TABLE +
                " WHERE workflowId = ?), " +
                "workflowTags = ? " +
                "WHERE workflowId = ?"
            );
    
    workflowUpdateNumCompletedPS =
        connection.prepareStatement(
            "UPDATE " +
                WORKFLOW_TABLE +
                " SET " +
                "lastUpdateTime = ?, " +
                "duration = ? - (SELECT startTime FROM " +
                WORKFLOW_TABLE +
                " WHERE workflowId = selectid), " +
                "numJobsCompleted = rows " +
            "FROM (SELECT count(*) as rows, workflowId as selectid FROM " +
                APPLICATION_TABLE +
                " WHERE workflowId = (SELECT workflowId FROM " +
                APPLICATION_TABLE +
                " WHERE appId = ?) " +
                "GROUP BY workflowId) as appsummary " +
            "WHERE workflowId = selectid"
            );
    
    appCompletedPS = 
        connection.prepareStatement(
            "UPDATE " +
                APPLICATION_TABLE +
                " SET " +
                "finishTime = ?, " +
                "status = ? " +
                "WHERE " +
                "appId = ?" 
            );

    appLaunchedPS =
        connection.prepareStatement(
            "UPDATE " +
                APPLICATION_TABLE +
                " SET " +
                "launchTime = ? " +
                "WHERE " +
                "appId = ?" 
            );
  }

  @Override
  protected void closeInternal() throws Exception {
    connection.close();
  }

  @Override
  public void applicationSubmitted(RMHistoryAppSubmittedEvent historyEvent) {
    LOG.info("Application submitted");
  
    try {
      String appId = historyEvent.getApplicationId().toString();
      appSubmittedPS.setString(1, appId);
      appSubmittedPS.setString(2, historyEvent.getAppName());
      appSubmittedPS.setString(3, historyEvent.getAppType());
      appSubmittedPS.setString(4, historyEvent.getUser());
      appSubmittedPS.setString(5, historyEvent.getQueue());
      appSubmittedPS.setLong(6, historyEvent.getSubmitTime());
      appSubmittedPS.setString(9, historyEvent.getAppInfo());
      
      WorkflowContext workflowContext = buildWorkflowContext(historyEvent);
      
      // Get workflow information
      boolean insertWorkflow = false;
      String existingContextString = null;
      String existingTagString = null;
      
      ResultSet rs = null;
      try {
        workflowSelectPS.setString(1, workflowContext.getWorkflowId());
        workflowSelectPS.execute();
        rs = workflowSelectPS.getResultSet();
        if (rs.next()) {
          existingContextString = rs.getString(1);
          existingTagString = rs.getString(2);
        } else {
          insertWorkflow = true;
        }
      } catch (SQLException sqle) {
        LOG.warn("workflow select failed with: ", sqle);
        insertWorkflow = false;
      } finally {
        try {
          if (rs != null)
            rs.close();
        } catch (SQLException e) {
          LOG.error("Exception while closing ResultSet", e);
        }
      }
  
      // Insert workflow 
      if (insertWorkflow) {
        workflowPS.setString(1, workflowContext.getWorkflowId());
        workflowPS.setString(2, workflowContext.getWorkflowName());
        workflowPS.setString(3, getWorkflowString(getSanitizedWorkflow(workflowContext, null)));
        workflowPS.setString(4, historyEvent.getUser());
        workflowPS.setLong(5, historyEvent.getSubmitTime());
        workflowPS.setLong(6, historyEvent.getSubmitTime());
        workflowPS.setLong(7, workflowContext.getWorkflowDag().size());
        workflowPS.setString(8, workflowContext.getWorkflowTags());
        workflowPS.executeUpdate();
        LOG.debug("Successfully inserted workflowId = " + 
            workflowContext.getWorkflowId());
      } else {
        ObjectMapper om = new ObjectMapper();
        WorkflowContext existingWorkflowContext = null;
        try {
          if (existingContextString != null)
            existingWorkflowContext = om.readValue(existingContextString.getBytes(), WorkflowContext.class);
        } catch (IOException e) {
          LOG.warn("Couldn't read existing workflow context for " + workflowContext.getWorkflowId() + " " + existingContextString, e);
        }
        
        WorkflowContext sanitizedWC = getSanitizedWorkflow(workflowContext, existingWorkflowContext);
        workflowUpdateTimePS.setString(1, getWorkflowString(sanitizedWC));
        workflowUpdateTimePS.setLong(2, sanitizedWC.getWorkflowDag().size());
        workflowUpdateTimePS.setLong(3, historyEvent.getSubmitTime());
        workflowUpdateTimePS.setLong(4, historyEvent.getSubmitTime());
        workflowUpdateTimePS.setString(5, workflowContext.getWorkflowId());
        workflowUpdateTimePS.setString(6, mergeTagLists(workflowContext.getWorkflowTags(), existingTagString));
        workflowUpdateTimePS.setString(7, workflowContext.getWorkflowId());
        workflowUpdateTimePS.executeUpdate();
        LOG.debug("Successfully updated workflowId = " + 
            workflowContext.getWorkflowId());
      }
  
      // Insert job
      appSubmittedPS.setString(7, workflowContext.getWorkflowId());
      appSubmittedPS.setString(8, workflowContext.getWorkflowEntityName());
      
      appSubmittedPS.executeUpdate();
      LOG.debug("Successfully inserted job = " + appId + 
          " and workflowId = " + workflowContext.getWorkflowId());
  
    } catch (SQLException sqle) {
      LOG.info("Failed to store " + historyEvent.getEventType() + " for application" + 
          historyEvent.getApplicationId() + " into " + APPLICATION_TABLE, sqle);
    } catch (Exception e) {
      LOG.info("Failed to store " + historyEvent.getEventType() + " for application" + 
          historyEvent.getApplicationId() + " into " + APPLICATION_TABLE, e);
    }
  }

  private static WorkflowContext getSanitizedWorkflow(WorkflowContext workflowContext, WorkflowContext existingWorkflowContext) {
    WorkflowContext sanitizedWC = new WorkflowContext();
    if (existingWorkflowContext == null) {
      sanitizedWC.setWorkflowDag(workflowContext.getWorkflowDag());
      sanitizedWC.setParentWorkflowContext(workflowContext.getParentWorkflowContext());
    } else {
      sanitizedWC.setWorkflowDag(constructMergedDag(existingWorkflowContext, workflowContext));
      sanitizedWC.setParentWorkflowContext(existingWorkflowContext.getParentWorkflowContext());
    }
    return sanitizedWC;
  }

  private static String getWorkflowString(WorkflowContext sanitizedWC) {
    String sanitizedWCString = null;
    try {
      ObjectMapper om = new ObjectMapper();
      sanitizedWCString = om.writeValueAsString(sanitizedWC);
    } catch (IOException e) {
      e.printStackTrace();
      sanitizedWCString = "";
    }
    return sanitizedWCString;
  }

  public static WorkflowContext buildWorkflowContext(RMHistoryAppSubmittedEvent event) {
    WorkflowContext parsedContext;
    String rawContext = event.getWorkflowContext();
    if (rawContext == null || rawContext.isEmpty()) {
      parsedContext = new WorkflowContext();
    } else {
      ObjectMapper om = new ObjectMapper();
      try {
        parsedContext = om.readValue(StringUtils.unEscapeString(rawContext, StringUtils.ESCAPE_CHAR, '"').getBytes(), WorkflowContext.class);
      } catch (Exception e) {
        parsedContext = new WorkflowContext();
      }
    }
    
    String workflowId = parsedContext.getWorkflowId();
    if (workflowId == null || workflowId.isEmpty())
      parsedContext.setWorkflowId(event.getApplicationId().toString().
          replace("application_", event.getAppType().toLowerCase() + "_"));
    String workflowName = parsedContext.getWorkflowName();
    if (workflowName == null || workflowName.isEmpty())
      parsedContext.setWorkflowName(event.getAppName());
    String workflowEntityName = parsedContext.getWorkflowEntityName();
    if (workflowEntityName == null || workflowEntityName.isEmpty())
      parsedContext.setWorkflowEntityName(Integer.toString(
          event.getApplicationId().getId()));
    WorkflowDag workflowDag = parsedContext.getWorkflowDag();
    if (workflowDag == null)
      workflowDag = new WorkflowDag();
    if (workflowDag.size()==0) {
      WorkflowDagEntry dagEntry = new WorkflowDagEntry();
      dagEntry.setSource(parsedContext.getWorkflowEntityName());
      workflowDag.addEntry(dagEntry);
      parsedContext.setWorkflowDag(workflowDag);
    }
    return parsedContext;
  }
  
  public static void mergeEntries(Map<String, Set<String>> edges, List<WorkflowDagEntry> entries) {
    if (entries == null)
      return;
    for (WorkflowDagEntry entry : entries) {
      if (!edges.containsKey(entry.getSource()))
        edges.put(entry.getSource(), new TreeSet<String>());
      Set<String> targets = edges.get(entry.getSource());
      targets.addAll(entry.getTargets());
    }
  }

  public static WorkflowDag constructMergedDag(WorkflowContext workflowContext, WorkflowContext existingWorkflowContext) {
    Map<String, Set<String>> edges = new TreeMap<String, Set<String>>();
    if (existingWorkflowContext.getWorkflowDag() != null)
      mergeEntries(edges, existingWorkflowContext.getWorkflowDag().getEntries());
    if (workflowContext.getWorkflowDag() != null)
      mergeEntries(edges, workflowContext.getWorkflowDag().getEntries());
    WorkflowDag mergedDag = new WorkflowDag();
    for (Entry<String,Set<String>> edge : edges.entrySet()) {
      WorkflowDagEntry entry = new WorkflowDagEntry();
      entry.setSource(edge.getKey());
      entry.getTargets().addAll(edge.getValue());
      mergedDag.addEntry(entry);
    }
    return mergedDag;
  }
  
  public static String mergeTagLists(String workflowTags, String existingTagList) {
    if (workflowTags == null)
      workflowTags = "";
    if (existingTagList == null)
      return workflowTags;
    if (workflowTags.isEmpty())
      return existingTagList;
    if (workflowTags.equals(existingTagList) || workflowTags.startsWith(existingTagList))
      return workflowTags;
    else return existingTagList + "," + workflowTags;
  }

  @Override
  public void applicationLaunched(RMHistoryAppLaunchedEvent event) {
    LOG.info("Application launched");
    
    try {
    appLaunchedPS.setLong(1, event.getLaunchTime());
    appLaunchedPS.setString(2, event.getApplicationId().toString());
    appLaunchedPS.executeUpdate();
    } catch (SQLException sqle) {
      LOG.info("Failed to store " + event.getEventType() + " for application" + 
          event.getApplicationId() + " into " + APPLICATION_TABLE, sqle);
    } catch (Exception e) {
      LOG.info("Failed to store " + event.getEventType() + " for application" + 
          event.getApplicationId() + " into " + APPLICATION_TABLE, e);
    }

    
  }

  @Override
  public void applicationCompleted(RMHistoryAppCompletedEvent event) {
    LOG.info("Application completed");
    
    try {
    appCompletedPS.setLong(1, event.getFinishTime());
    appCompletedPS.setString(2, event.getState().toString());
    appCompletedPS.setString(3, event.getApplicationId().toString());
    appCompletedPS.executeUpdate();
    workflowUpdateNumCompletedPS.setLong(1, event.getFinishTime());
    workflowUpdateNumCompletedPS.setLong(2, event.getFinishTime());
    workflowUpdateNumCompletedPS.setString(3, event.getApplicationId().toString());
    workflowUpdateNumCompletedPS.executeUpdate();
    } catch (SQLException sqle) {
      LOG.info("Failed to store " + event.getEventType() + " for application" + 
          event.getApplicationId() + " into " + APPLICATION_TABLE, sqle);
    } catch (Exception e) {
      LOG.info("Failed to store " + event.getEventType() + " for application" + 
          event.getApplicationId() + " into " + APPLICATION_TABLE, e);
    }

  }

}
