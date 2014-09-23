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

package org.apache.hadoop.yarn.registry.server.services;


import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.services.RegistryBindingSource;
import org.apache.hadoop.yarn.registry.client.services.RegistryOperationsService;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Administrator service for the registry. This is the one with
 * permissions to create the base directories and those for users.
 * 
 * It also includes support for asynchronous operations, especially cleanup
 * actions.
 * 
 * It does not contain any policy about when/how to invoke these.
 */
public class RegistryAdminService extends RegistryOperationsService {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryAdminService.class);

  protected final ExecutorService executor;
  
  public RegistryAdminService(String name) {
    this(name, null);
  }

  public RegistryAdminService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
    executor = Executors.newCachedThreadPool(
        new ThreadFactory() {
          AtomicInteger counter = new AtomicInteger(1);

          @Override
          public Thread newThread(Runnable r) {
            return new Thread(r,
                "RegistryAdminService " + counter.getAndIncrement());
          }
        });
  }

  /**
   * Stop the service: halt the executor. 
   * @throws Exception exception.
   */
  @Override
  protected void serviceStop() throws Exception {
    stopExecutor();
    super.serviceStop();
  }

  /**
   * Stop the executor if it is not null.
   * This uses {@link ExecutorService#shutdownNow()}
   * and so does not block until they have completed.
   */
  protected synchronized void stopExecutor() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  /**
   * Get the executor
   * @return the executor
   */
  protected ExecutorService getExecutor() {
    return executor;
  }

  /**
   * Submit a callable
   * @param callable callable
   * @param <V> type of the final get
   * @return a future to wait on
   */
  public <V> Future<V> submit(Callable<V> callable) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Submitting {}", callable);
    }
    return getExecutor().submit(callable);
  }

  /**
   * Asynchronous operation to create a directory
   * @param path path
   * @param acls ACL list
   * @param createParents flag to indicate parent dirs should be created
   * as needed
   * @throws IOException
   */
  public void createDirAsync(final String path,
      final List<ACL> acls,
      final boolean createParents) throws IOException {
    submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        maybeCreate(path, CreateMode.PERSISTENT,
            acls, createParents);
        return null;
      }
    });
    
  }

  /**
   * Policy to purge entries
   */
  public enum PurgePolicy {
    PurgeAll,
    FailOnChildren,
    SkipOnChildren
  }


  /**
   * Recursive operation to purge all matching records under a base path.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   *
   * @param path base path
   * @param id ID for service record.id
   * @param persistencePolicyMatch ID for the persistence policy to match: no match, no delete.
   * If set to to -1 or below, " don't check"
   * @param purgePolicy what to do if there is a matching record with children
   * @return the number of calls to the zkDelete() operation. This is purely for
   * testing.
   * @throws IOException problems
   * @throws PathIsNotEmptyDirectoryException if an entry cannot be deleted
   * as his children and the purge policy is FailOnChildren
   */
  @VisibleForTesting
  public int purge(String path,
      NodeSelector selector,
      PurgePolicy purgePolicy,
      BackgroundCallback callback) throws IOException {

    // list this path's children
    RegistryPathStatus[] entries = list(path);
    RegistryPathStatus registryPathStatus = stat(path);

    boolean toDelete = false;
    // look at self to see if it has a service record
    try {
      ServiceRecord serviceRecord = resolve(path);
      // there is now an entry here.
      toDelete = selector.shouldSelect(path, registryPathStatus, serviceRecord);
    } catch (EOFException ignored) {
      // ignore
    } catch (InvalidRecordException ignored) {
      // ignore
    }

    if (toDelete && entries.length > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Match on record @ {} with children ", path);
      }
      // there's children
      switch (purgePolicy) {
        case SkipOnChildren:
          // don't do the deletion... continue to next record
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping deletion");
          }
          toDelete = false;
          break;
        case PurgeAll:
          // mark for deletion
          if (LOG.isDebugEnabled()) {
            LOG.debug("Scheduling for deletion with children");
          }
          toDelete = true;
          entries = new RegistryPathStatus[0];
          break;
        case FailOnChildren:
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failing deletion operation");
          }
          throw new PathIsNotEmptyDirectoryException(path);
      }
    }

    int deleteOps = 0;
    if (toDelete) {
      deleteOps++;
      zkDelete(path, true, callback);
    }

    // now go through the children
    for (RegistryPathStatus status : entries) {
      deleteOps += purge(status.path,
          selector,
          purgePolicy,
          callback);
    }

    return deleteOps;
  }
  
  /**
   * Comparator used for purge logic
   */
  public interface NodeSelector {

    boolean shouldSelect(String path,
        RegistryPathStatus registryPathStatus,
        ServiceRecord serviceRecord);
  }


  /**
   * An async registry purge action taking 
   * a selector which decides what to delete
   */
  public class AsyncPurge implements Callable<Integer> {

    private final BackgroundCallback callback;
    private final NodeSelector selector;
    private final String path;
    private final PurgePolicy purgePolicy;

    public AsyncPurge(String path,
        NodeSelector selector,
        PurgePolicy purgePolicy,
        BackgroundCallback callback) {
      this.callback = callback;
      this.selector = selector;
      this.path = path;
      this.purgePolicy = purgePolicy;
    }


    @Override
    public Integer call() throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Executing {}", this);
      }
      return purge(path,
          selector,
          purgePolicy,
          callback);
    }

    @Override
    public String toString() {
      return String.format(
          "Record purge under %s with selector %s",
          path, selector);
    }
  }

}
