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

package org.apache.hadoop.yarn.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;

public abstract class AbstractService implements Service {

  private static final Log LOG = LogFactory.getLog(AbstractService.class);

  /**
   * Service state: initially {@link STATE#NOTINITED}.
   */
  private STATE state = STATE.NOTINITED;

  /**
   * Service name
   */
  private final String name;
  /**
   * Service start time. Will be zero until the service is started.
   */
  private long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private Configuration config;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  private final List<ServiceStateChangeListener> listeners =
    new ArrayList<ServiceStateChangeListener>();

  /**
   * Construct the service. 
   * @param name service name
   */
  public AbstractService(String name) {
    this.name = name;
  }

  @Override
  public synchronized STATE getServiceState() {
    return state;
  }

  /**
   * {@inheritDoc}
   * @throws IllegalStateException if the current service state does not permit
   * this action
   */
  @Override
  public synchronized void init(Configuration conf) {
    ensureCurrentState(STATE.NOTINITED);
    this.config = conf;
    changeState(STATE.INITED);
    LOG.info("Service:" + getName() + " is inited.");
  }

  /**
   * {@inheritDoc}
   * @throws IllegalStateException if the current service state does not permit
   * this action
   */
  @Override
  public synchronized void start() {
    ensureCurrentState(STATE.INITED);
    startTime = System.currentTimeMillis();
    changeState(STATE.STARTED);
    LOG.info("Service:" + getName() + " is started.");
  }

  /**
   * {@inheritDoc}
   * @throws IllegalStateException if the current service state does not permit
   * this action
   */
  @Override
  public synchronized void stop() {
    if (state == STATE.STOPPED ||
        state == STATE.INITED ||
        state == STATE.NOTINITED) {
      // already stopped, or else it was never
      // started (eg another service failing canceled startup)
      return;
    }
    ensureCurrentState(STATE.STARTED);
    changeState(STATE.STOPPED);
    LOG.info("Service:" + getName() + " is stopped.");
  }

  @Override
  public synchronized void register(ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public synchronized void unregister(ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public synchronized Configuration getConfig() {
    return config;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  /**
   * Verify that that a service is in a given state. 
   * @param currentState the desired state
   * @throws IllegalStateException if the service state is different from 
   * the desired state
   */
  private void ensureCurrentState(STATE currentState) {
    if (state != currentState) {
      throw new IllegalStateException("For this operation, current State must " +
        "be " + currentState + " instead of " + state);
    }
  }

  /**
   * Change to a new state and notify all listeners.
   * This is a private method that is only invoked from synchronized methods, 
   * which avoid having to clone the listener list. It does imply that
   * the state change listener methods should be short lived, as they
   * will delay the state transition.
   * @param newState new service state
   */
  private void changeState(STATE newState) {
    state = newState;
    //notify listeners
    for (ServiceStateChangeListener l : listeners) {
      l.stateChanged(this);
    }
  }


  /**
   * Provide the service name and state in the toString() operator.
   * @return the service name and state
   */
  @Override
  public String toString() {
    return "Service " + getName() + " in state " + state;
  }

  /**
   * Helper method for safely interrupting threads during shutdown. 
   * If the target is non null, it will be interrupted. The return
   * value will always be null. This permits a use such as
   * <pre>
   *   workerThread = interruptThread(workerThread);
   * </pre>
   * Such a line will interrupt the worker thread if it is not null, 
   * and guarantee that the field will be null afterwards.
   * @param target target thread -this may be null
   * @return null, always
   */
  protected final Thread interruptThread(Thread target) {
    if (target != null) {
      target.interrupt();
    }
    return null;
  }

  /**
   * Stop an IPC server if not null
   * @param target the target server
   * @return null, always
   */
  protected final Server stopIPCServer(Server target) {
    if (target!=null) {
      target.stop();
    }
    return null;
  }

  /**
  * Stop a service; if it is null do nothing
  *
  * @param service a service
  */
  public static void stopService(Service service) {
    if (service != null) {
      service.stop();
    }
  }

  /**
   * Stop a service; if it is null do nothing. Exceptions are caught and logged
   * (but not Throwables). This operation is intended to be used in cleanup
   * operations
   *
   * @param service a service; may be null
   */
  public static void stopServiceQuietly(Service service) {
    if (service != null) {
      try {
        service.stop();
      } catch (Exception e) {
        LOG.warn("When stopping the service " + service.getName()
                     + " : " + e,
                 e);
      }
    }
  }

}
