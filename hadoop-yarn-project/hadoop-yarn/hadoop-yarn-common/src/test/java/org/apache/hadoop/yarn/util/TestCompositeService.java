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

package org.apache.hadoop.yarn.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.apache.hadoop.yarn.service.ServiceStateException;
import org.junit.Before;
import org.junit.Test;

public class TestCompositeService {

  private static final int NUM_OF_SERVICES = 5;

  private static final int FAILED_SERVICE_SEQ_NUMBER = 2;

  private static final Log LOG  = LogFactory.getLog(TestCompositeService.class);

  /**
   * flag to state policy of CompositeService, and hence 
   * what to look for after trying to stop a service from another state
   * (e.g inited)
   */
  private static final boolean STOP_ONLY_STARTED_SERVICES =
    CompositeServiceImpl.isPolicyToStopOnlyStartedServices();
  
  @Before
  public void setup() {
    CompositeServiceImpl.resetCounter();
  }

  @Test
  public void testCallSequence() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
        new CompositeServiceImpl[0]);

    assertEquals("Number of registered services ", NUM_OF_SERVICES,
        services.length);

    Configuration conf = new Configuration();
    // Initialise the composite service
    serviceManager.init(conf);

    //verify they were all inited
    assertInState(STATE.INITED, services);

    // Verify the init() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals("For " + services[i]
          + " service, init() call sequence number should have been ", i,
          services[i].getCallSequenceNumber());
    }

    // Reset the call sequence numbers
    resetServices(services);

    serviceManager.start();
    //verify they were all started
    assertInState(STATE.STARTED, services);

    // Verify the start() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals("For " + services[i]
          + " service, start() call sequence number should have been ", i,
          services[i].getCallSequenceNumber());
    }
    resetServices(services);


    serviceManager.stop();
    //verify they were all stopped
    assertInState(STATE.STOPPED, services);

    // Verify the stop() call sequence numbers for every service
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals("For " + services[i]
          + " service, stop() call sequence number should have been ",
          ((NUM_OF_SERVICES - 1) - i), services[i].getCallSequenceNumber());
    }

    // Try to stop again. This should be a no-op.
    serviceManager.stop();
    // Verify that stop() call sequence numbers for every service don't change.
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      assertEquals("For " + services[i]
          + " service, stop() call sequence number should have been ",
          ((NUM_OF_SERVICES - 1) - i), services[i].getCallSequenceNumber());
    }
  }

  private void resetServices(CompositeServiceImpl[] services) {
    // Reset the call sequence numbers
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      services[i].reset();
    }
  }

  @Test
  public void testServiceStartup() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      if (i == FAILED_SERVICE_SEQ_NUMBER) {
        service.setThrowExceptionOnStart(true);
      }
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
        new CompositeServiceImpl[0]);

    Configuration conf = new Configuration();

    // Initialise the composite service
    serviceManager.init(conf);

    // Start the composite service
    try {
      serviceManager.start();
      fail(
        "Exception should have been thrown due to startup failure of last service");
    } catch (YarnException e) {
      if (STOP_ONLY_STARTED_SERVICES) {
        for (int i = 0; i < NUM_OF_SERVICES - 1; i++) {
          //everything already started is stopped
          assertInState(STATE.STOPPED, services,0,FAILED_SERVICE_SEQ_NUMBER);
          //the service that failed has also been stopped
          assertInState(STATE.STOPPED, services[FAILED_SERVICE_SEQ_NUMBER]);
          //the not-yet started services are left alone
          assertInState(STATE.INITED, services,FAILED_SERVICE_SEQ_NUMBER+1,
                        NUM_OF_SERVICES);
        }
      } else {
         //all services have been stopped
        assertInState(STATE.STOPPED, services);
      }
    }

  }

  @Test
  public void testServiceStop() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      if (i == FAILED_SERVICE_SEQ_NUMBER) {
        service.setThrowExceptionOnStop(true);
      }
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
        new CompositeServiceImpl[0]);

    Configuration conf = new Configuration();

    // Initialise the composite service
    serviceManager.init(conf);

    serviceManager.start();

    // Stop the composite service
    try {
      serviceManager.stop();
    } catch (YarnException e) {

    }
    assertInState(STATE.STOPPED, services);
  }

  /**
   * Assert that all services are in the same expected state
   * @param expected expected state value
   * @param services services to examine
   */
  private void assertInState(STATE expected, CompositeServiceImpl[] services) {
    assertInState(expected, services,0, services.length);
  }

  /**
   * Assert that all services are in the same expected state
   * @param expected expected state value
   * @param services services to examine
   * @param start start offset
   * @param finish finish offset: the count stops before this number
   */
  private void assertInState(STATE expected,
                             CompositeServiceImpl[] services,
                             int start, int finish) {
    for (int i = start; i < finish; i++) {
      Service service = services[i];
      assertInState(expected, service);
    }
  }

  private void assertInState(STATE expected, Service service) {
    assertEquals("Service state should have been " + expected + " in "
                 + service,
                 expected,
                 service.getServiceState());
  }

  /**
   * Shut down from not-inited
   */
  @Test
  public void testServiceStopFromNotInited() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
      new CompositeServiceImpl[0]);
    serviceManager.stop();
    if (STOP_ONLY_STARTED_SERVICES) {
      //this policy => no services were stopped
      assertInState(STATE.NOTINITED, services);
    } else {
      //this policy => all services were stopped in reverse order
      assertInState(STATE.STOPPED, services);
      // Verify the stop() call sequence numbers for every service
      for (int i = 0; i < NUM_OF_SERVICES; i++) {
        assertEquals("For " + services[i]
                     +
                     " service, stop() call sequence number should have been ",
                     ((NUM_OF_SERVICES - 1) - i),
                     services[i].getCallSequenceNumber());
      }
    }
  }

  /**
   * Shut down from inited
   */
  @Test
  public void testServiceStopFromInited() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");

    // Add services
    for (int i = 0; i < NUM_OF_SERVICES; i++) {
      CompositeServiceImpl service = new CompositeServiceImpl(i);
      serviceManager.addTestService(service);
    }

    CompositeServiceImpl[] services = serviceManager.getServices().toArray(
      new CompositeServiceImpl[0]);
    serviceManager.init(new Configuration());
    serviceManager.stop();
    if (STOP_ONLY_STARTED_SERVICES) {
      //this policy => no services were stopped
      assertInState(STATE.INITED, services);
    } else {
      assertInState(STATE.STOPPED, services);
    }
  }

  /**
   * Use a null configuration & expect a failure
   * @throws Throwable
   */
  @Test
  public void testInitNullConf() throws Throwable {
    ServiceManager serviceManager = new ServiceManager("testInitNullConf");

    CompositeServiceImpl service = new CompositeServiceImpl(0);
    serviceManager.addTestService(service);
    try {
      serviceManager.init(null);
      LOG.warn("Null Configurations are permitted " + serviceManager);
    } catch (ServiceStateException e) {
      //expected
    }
  }

  /**
   * Walk the service through their lifecycle without any children;
   * verify that it all works.
   */
  @Test
  public void testServiceLifecycleNoChildrenl() {
    ServiceManager serviceManager = new ServiceManager("ServiceManager");
    serviceManager.init(new Configuration());
    serviceManager.start();
    serviceManager.stop();
  }

    public static class CompositeServiceImpl extends CompositeService {

    public static boolean isPolicyToStopOnlyStartedServices() {
      return STOP_ONLY_STARTED_SERVICES;
    }

    private static int counter = -1;

    private int callSequenceNumber = -1;

    private boolean throwExceptionOnStart;

    private boolean throwExceptionOnStop;

    public CompositeServiceImpl(int sequenceNumber) {
      super(Integer.toString(sequenceNumber));
    }

    @Override
    protected void innerInit(Configuration conf) throws Exception {
      counter++;
      callSequenceNumber = counter;
      super.innerInit(conf);
    }

    @Override
    protected void innerStart() throws Exception {
      if (throwExceptionOnStart) {
        throw new YarnException("Fake service start exception");
      }
      counter++;
      callSequenceNumber = counter;
      super.innerStart();
    }

    @Override
    protected void innerStop() throws Exception {
      counter++;
      callSequenceNumber = counter;
      if (throwExceptionOnStop) {
        throw new YarnException("Fake service stop exception");
      }
      super.innerStop();
    }

    public static int getCounter() {
      return counter;
    }

    public int getCallSequenceNumber() {
      return callSequenceNumber;
    }

    public void reset() {
      callSequenceNumber = -1;
      counter = -1;
    }

    public static void resetCounter() {
      counter = -1;
    }

    public void setThrowExceptionOnStart(boolean throwExceptionOnStart) {
      this.throwExceptionOnStart = throwExceptionOnStart;
    }

    public void setThrowExceptionOnStop(boolean throwExceptionOnStop) {
      this.throwExceptionOnStop = throwExceptionOnStop;
    }

    @Override
    public String toString() {
      return "Service " + getName();
    }

  }

  public static class ServiceManager extends CompositeService {

    public void addTestService(CompositeService service) {
      addService(service);
    }

    public ServiceManager(String name) {
      super(name);
    }
  }

}
