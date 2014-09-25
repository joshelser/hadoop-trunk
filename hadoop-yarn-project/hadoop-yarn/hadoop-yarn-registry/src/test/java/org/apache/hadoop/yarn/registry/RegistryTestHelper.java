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

package org.apache.hadoop.yarn.registry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.registry.client.api.RegistryConstants;
import org.apache.hadoop.yarn.registry.client.binding.RecordOperations;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.types.AddressTypes;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ProtocolTypes;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.registry.secure.AbstractSecureRegistryTest;
import org.apache.zookeeper.common.PathUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.inetAddrEndpoint;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.ipcEndpoint;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.restEndpoint;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.tuple;
import static org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils.webEndpoint;

/**
 * This is a set of static methods to aid testing the registry operations.
 * The methods can be imported statically —or the class used as a base
 * class for tests. 
 */
public class RegistryTestHelper extends Assert {
  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String USER = "devteam/";
  public static final String NAME = "hdfs";
  public static final String API_WEBHDFS = "org_apache_hadoop_namenode_webhdfs";
  public static final String API_HDFS = "org_apache_hadoop_namenode_dfs";
  public static final String USERPATH = RegistryConstants.PATH_USERS + USER;
  public static final String PARENT_PATH = USERPATH + SC_HADOOP + "/";
  public static final String ENTRY_PATH = PARENT_PATH + NAME;
  public static final String NNIPC = "nnipc";
  public static final String IPC2 = "IPC2";
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryTestHelper.class);
  public static final String KTUTIL = "ktutil";
  private static final RecordOperations.ServiceRecordMarshal recordMarshal =
      new RecordOperations.ServiceRecordMarshal();

  /**
   * Assert the path is valid by ZK rules
   * @param path path to check
   */
  public static void assertValidZKPath(String path) {
    try {
      PathUtils.validatePath(path);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Path " + path + ": " + e, e);
    }
  }

  /**
   * Assert that a string is not empty (null or "")
   * @param message message to raise if the string is empty
   * @param check string to check
   */
  public static void assertNotEmpty(String message, String check) {
    if (StringUtils.isEmpty(check)) {
      fail(message);
    }
  }

  /**
   * Assert that a string is empty (null or "")
   * @param check string to check
   */
  public static void assertNotEmpty(String check) {
    if (StringUtils.isEmpty(check)) {
      fail("Empty string");
    }
  }

  /**
   * Log the details of a login context
   * @param name name to assert that the user is logged in as
   * @param loginContext the login context
   */
  public static void logLoginDetails(String name,
      LoginContext loginContext) {
    assertNotNull("Null login context", loginContext);
    Subject subject = loginContext.getSubject();
    LOG.info("Logged in as {}:\n {}", name, subject);
  }

  /**
   * Set the JVM property to enable Kerberos debugging
   */
  public static void enableKerberosDebugging() {
    System.setProperty(AbstractSecureRegistryTest.SUN_SECURITY_KRB5_DEBUG,
        "true");
  }
  /**
   * Set the JVM property to enable Kerberos debugging
   */
  public static void disableKerberosDebugging() {
    System.setProperty(AbstractSecureRegistryTest.SUN_SECURITY_KRB5_DEBUG,
        "false");
  }

  /**
   * General code to validate bits of a component/service entry built iwth
   * {@link #addSampleEndpoints(ServiceRecord, String)}
   * @param record instance to check
   */
  public static void validateEntry(ServiceRecord record) {
    assertNotNull("null service record", record);
    List<Endpoint> endpoints = record.external;
    assertEquals(2, endpoints.size());

    Endpoint webhdfs = findEndpoint(record, API_WEBHDFS, true, 1, 1);
    assertEquals(API_WEBHDFS, webhdfs.api);
    assertEquals(AddressTypes.ADDRESS_URI, webhdfs.addressType);
    assertEquals(ProtocolTypes.PROTOCOL_REST, webhdfs.protocolType);
    List<List<String>> addressList = webhdfs.addresses;
    List<String> url = addressList.get(0);
    String addr = url.get(0);
    assertTrue(addr.contains("http"));
    assertTrue(addr.contains(":8020"));

    Endpoint nnipc = findEndpoint(record, NNIPC, false, 1,2);
    assertEquals("wrong protocol in " + nnipc, ProtocolTypes.PROTOCOL_THRIFT,
        nnipc.protocolType);

    Endpoint ipc2 = findEndpoint(record, IPC2, false, 1,2);

    Endpoint web = findEndpoint(record, "web", true, 1, 1);
    assertEquals(1, web.addresses.size());
    assertEquals(1, web.addresses.get(0).size());
  }

  /**
   * Assert that an endpoint matches the criteria
   * @param endpoint endpoint to examine
   * @param addressType expected address type
   * @param protocolType expected protocol type
   * @param api API
   */
  public static void assertMatches(Endpoint endpoint,
      String addressType,
      String protocolType,
      String api) {
    assertNotNull(endpoint);
    assertEquals(addressType, endpoint.addressType);
    assertEquals(protocolType, endpoint.protocolType);
    assertEquals(api, endpoint.api);
  }

  /**
   * Assert the records match. Only the ID, registration time,
   * description and persistence are checked —not endpoints.
   * @param source record that was written
   * @param resolved the one that resolved.
   */
  public static void assertMatches(ServiceRecord source, ServiceRecord resolved) {
    assertNotNull("Null source record ", source);
    assertNotNull("Null resolved record ", resolved);
    assertEquals(source.yarn_id, resolved.yarn_id);
    assertEquals(source.registrationTime, resolved.registrationTime);
    assertEquals(source.description, resolved.description);
    assertEquals(source.yarn_persistence, resolved.yarn_persistence);
  }

  /**
   * Find an endpoint in a record or fail,
   * @param record   record
   * @param api API
   * @param external external?
   * @param addressElements expected # of address elements?
   * @param addressTupleSize expected size of a type
   * @return the endpoint.
   */
  public static Endpoint findEndpoint(ServiceRecord record,
      String api, boolean external, int addressElements, int addressTupleSize) {
    Endpoint epr = external ? record.getExternalEndpoint(api)
                            : record.getInternalEndpoint(api);
    if (epr != null) {
      assertEquals("wrong # of addresses",
          addressElements, epr.addresses.size());
      assertEquals("wrong # of elements in an address tuple",
          addressTupleSize, epr.addresses.get(0).size());
      return epr;
    }
    List<Endpoint> endpoints = external ? record.external : record.internal;
    StringBuilder builder = new StringBuilder();
    for (Endpoint endpoint : endpoints) {
      builder.append("\"").append(endpoint).append("\" ");
    }
    fail("Did not find " + api + " in endpoints " + builder);
    // never reached; here to keep the compiler happy
    return null;
  }

  /**
   * Log a record
   * @param name record name
   * @param record details
   * @throws IOException only if something bizarre goes wrong marshalling
   * a record.
   */
  public static void logRecord(String name, ServiceRecord record) throws
      IOException {
    LOG.info(" {} = \n{}\n", name, recordMarshal.toJson(record));
  }


  /**
   * Create a service entry with the sample endpoints
   * @param persistence persistence policy
   * @return the record
   * @throws IOException on a failure
   */
  public static ServiceRecord buildExampleServiceEntry(int persistence) throws
      IOException,
      URISyntaxException {
    ServiceRecord record = new ServiceRecord();
    record.yarn_id = "example-0001";
    record.yarn_persistence = persistence;
    record.registrationTime = System.currentTimeMillis();
    addSampleEndpoints(record, "namenode");
    return record;
  }

  /**
   * Add some endpoints
   * @param entry entry
   */
  public static void addSampleEndpoints(ServiceRecord entry, String hostname)
      throws URISyntaxException {
    entry.addExternalEndpoint(webEndpoint("web",
        new URI("http", hostname + ":80", "/")));
    entry.addExternalEndpoint(
        restEndpoint(API_WEBHDFS,
            new URI("http", hostname + ":8020", "/")));

    Endpoint endpoint = ipcEndpoint(API_HDFS,
        true, null);
    endpoint.addresses.add(tuple(hostname, "8030"));
    entry.addInternalEndpoint(endpoint);
    InetSocketAddress localhost = new InetSocketAddress("localhost", 8050);
    entry.addInternalEndpoint(
        inetAddrEndpoint(NNIPC, ProtocolTypes.PROTOCOL_THRIFT, "localhost",
            8050));
    entry.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint(
            IPC2,
            true,
            RegistryTypeUtils.marshall(localhost)));
  }

  /**
   * Describe the stage in the process with a box around it -so as
   * to highlight it in test logs
   * @param log log to use
   * @param text text
   * @param args logger args
   */
  public static void describe(Logger log, String text, Object...args) {
    log.info("\n=======================================");
    log.info(text, args);
    log.info("=======================================\n");
  }


  /**
   * log out from a context if non-null ... exceptions are caught and logged
   * @param login login context
   * @return null, always
   */
  public static LoginContext logout(LoginContext login) {
    try {
      if (login != null) {
        LOG.debug("Logging out login context {}", login.toString());
        login.logout();
      }
    } catch (LoginException e) {
      LOG.warn("Exception logging out: {}", e, e);
    }
    return null;
  }

  /**
   * Exec the native <code>ktutil</code> to list the keys
   * (primarily to verify that the generated keytabs are compatible).
   * This operation is not executed on windows. On other platforms
   * it requires <code>ktutil</code> to be installed and on the path
   * <pre>
   *   ktutil --keytab=target/kdc/zookeeper.keytab list --keys
   * </pre>
   * @param keytab keytab to list
   * @throws IOException on any execution problem, including the executable
   * being missing
   */
  public static String ktList(File keytab) throws IOException {
    if (!Shell.WINDOWS) {
      String path = keytab.getAbsolutePath();
      String out = Shell.execCommand(
          KTUTIL,
          "--keytab=" + path,
          "list",
          "--keys"
      );
      LOG.info("Listing of keytab {}:\n{}\n", path, out);
      return out;
    }
    return "";
  }

  /**
   * Perform a robust <code>ktutils -l</code> ... catches and ignores
   * exceptions, otherwise the output is logged.
   * @param keytab keytab to list
   * @return the result of the operation, or "" on any problem
   */
  public static String ktListRobust(File keytab) {
    try {
      return ktList(keytab);
    } catch (IOException e) {
      // probably not on the path
      return "";
    }
  }

  /**
   * Login via a UGI. Requres UGI to have been set up
   * @param user username
   * @param keytab keytab to list
   * @return the UGI
   * @throws IOException
   */
  protected UserGroupInformation loginUGI(String user, File keytab) throws
      IOException {
    LOG.info("Logging in as {} from {}", user, keytab);
    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user,
        keytab.getAbsolutePath());
  }
}
