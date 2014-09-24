/*
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

package org.apache.hadoop.yarn.registry.client.services;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;

import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils.*;

import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.yarn.registry.client.api.CreateFlags;
import org.apache.hadoop.yarn.registry.client.services.zk.CuratorService;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The YARN ZK registry operations service.
 *
 * It implements the {@link RegistryOperations} API by mapping the commands
 * to zookeeper operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryOperationsService extends CuratorService 
  implements RegistryOperations {

  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryOperationsService.class);

  private final RecordOperations.ServiceRecordMarshal serviceRecordMarshal
      = new RecordOperations.ServiceRecordMarshal();

  public RegistryOperationsService(String name) {
    this(name, null);
  }

  public RegistryOperationsService() {
    this("RegistryOperationsService");
  }

  public RegistryOperationsService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
  }

  /**
   * Get the aggregate set of ACLs the client should use
   * to create directories
   * @return the ACL list
   */
  public List<ACL> getClientAcls() {
    return getRegistrySecurity().getClientACLs();
  }

  /**
   * Validate a path ... this includes checking that they are DNS-valid
   * @param path path to validate
   * @throws InvalidPathnameException if a path is considered invalid
   */
  protected void validatePath(String path) throws InvalidPathnameException {
    RegistryPathUtils.validateElementsAsDNS(path);
  }
  
  @Override
  public boolean mknode(String path, boolean createParents) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    return zkMkPath(path, CreateMode.PERSISTENT, createParents, getClientAcls());
  }

  @Override
  public void create(String path,
      ServiceRecord record,
      int createFlags) throws
      PathNotFoundException,
      FileAlreadyExistsException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    Preconditions.checkArgument(record != null, "null record");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(record.id), 
        "empty record ID");
    validatePath(path);
    LOG.info("Registered at {} : {}", path, record);

    CreateMode mode = CreateMode.PERSISTENT;
    byte[] bytes = serviceRecordMarshal.toByteswithHeader(record);
    zkSet(path, mode, bytes, getClientAcls(),
        ((createFlags & CreateFlags.OVERWRITE) != 0));
  }

  @Override
  public ServiceRecord resolve(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    byte[] bytes = zkRead(path);
    return serviceRecordMarshal.fromBytesWithHeader(path, bytes);
  }

  @Override
  public boolean exists(String path) throws IOException {
    validatePath(path);
    return zkPathExists(path);
  }

  @Override
  public RegistryPathStatus stat(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    Stat stat = zkStat(path);
    
    RegistryPathStatus status = new RegistryPathStatus(
        path,
        stat.getCtime(),
        stat.getDataLength(),
        stat.getNumChildren());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stat {} => {}", path, status);
    }
    return status;
  }

  @Override
  public List<RegistryPathStatus> list(String path) throws
      PathNotFoundException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    List<String> childNames = zkList(path);
    int size = childNames.size();
    List<RegistryPathStatus> childList =
        new ArrayList<RegistryPathStatus>(size);
    for (String childName : childNames) {
      childList.add(stat(join(path, childName)));
    }
    return childList;
  }

  @Override
  public void delete(String path, boolean recursive) throws
      PathNotFoundException,
      PathIsNotEmptyDirectoryException,
      AccessControlException,
      InvalidPathnameException,
      IOException {
    validatePath(path);
    zkDelete(path, recursive, null);
  }

}
