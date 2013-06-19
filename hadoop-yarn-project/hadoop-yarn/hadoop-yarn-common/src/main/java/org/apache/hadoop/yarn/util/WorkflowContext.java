/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;


@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@Private
@Unstable
public class WorkflowContext {
  
  private String workflowId;
  private String workflowName;
  private String workflowEntityName;
  private String workflowTags;
  
  private WorkflowDag workflowDag;
  
  private WorkflowContext parentWorkflowContext;
  
  public WorkflowContext() {
    /* Required by JAXB. */
  }
  
  /* Getters. */
  public String getWorkflowId() {
    return this.workflowId;
  }
  
  public String getWorkflowName() {
    return this.workflowName;
  }
  
  public String getWorkflowEntityName() {
    return this.workflowEntityName;
  }
  
  public String getWorkflowTags() {
    return workflowTags;
  }
  
  public WorkflowDag getWorkflowDag() {
    return this.workflowDag;
  }
  
  public WorkflowContext getParentWorkflowContext() {
    return this.parentWorkflowContext;
  }
  
  /* Setters. */
  public void setWorkflowId(String wfId) {
    this.workflowId = wfId;
  }
  
  public void setWorkflowName(String wfName) {
    this.workflowName = wfName;
  }
  
  public void setWorkflowEntityName(String wfEntityName) {
    this.workflowEntityName = wfEntityName;
  }
  
  public void setWorkflowTags(String workflowTags) {
    this.workflowTags = workflowTags;
  }
  
  public void setWorkflowDag(WorkflowDag wfDag) {
    this.workflowDag = wfDag;
  }
  
  public void setParentWorkflowContext(WorkflowContext pWfContext) {
    this.parentWorkflowContext = pWfContext;
  }

  public static String create(String workflowId, String workflowName, String workflowNodeName, String workflowTags, Map<String,String> workflowAdjacencies, int prefixLen) {
    ObjectMapper om = new ObjectMapper();
    WorkflowContext wc = new WorkflowContext();
    wc.setWorkflowId(workflowId);
    wc.setWorkflowName(workflowName);
    wc.setWorkflowEntityName(workflowNodeName);
    wc.setWorkflowTags(workflowTags);
    WorkflowDag dag = new WorkflowDag();
    for (Entry<String,String> entry : workflowAdjacencies.entrySet()) {
      WorkflowDag.WorkflowDagEntry dagEntry = new WorkflowDag.WorkflowDagEntry();
      int keyLen = entry.getKey().length();
      dagEntry.setSource(entry.getKey().substring(prefixLen, keyLen));
      String[] targets = StringUtils.split(entry.getValue());
      List<String> targetList = new ArrayList<String>(targets.length);
      for (String s : targets)
        targetList.add(s);
      dagEntry.setTargets(targetList);
      dag.addEntry(dagEntry);
    }
    wc.setWorkflowDag(dag);
    try {
      return StringUtils.escapeString(om.writeValueAsString(wc), StringUtils.ESCAPE_CHAR, '"');
    } catch (JsonGenerationException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return "";
  }
}