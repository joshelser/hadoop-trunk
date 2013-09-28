/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

/**
 * Test concat -if supported
 */
public abstract class AbstractAppendContractTest extends AbstractFSContractTestBase {
  public static final Log LOG = LogFactory.getLog(AbstractAppendContractTest.class);

  private Path testPath;
  private Path srcFile;
  private Path target;

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_APPEND);

    //delete the test directory
    testPath = path("test");
    target = new Path(testPath, "target");
  }

  @Override
  public void teardown() throws Exception {
    cleanup("teardown", getFileSystem(), testPath);
    super.teardown();
  }

  @Test
  public void testAppendToEmptyFile() throws Throwable {
    touch(getFileSystem(), target);
    byte[] dataset = dataset(256, 'a', 'z');
    FSDataOutputStream outputStream = getFileSystem().append(target);
    try {
      outputStream.write(dataset);
    } finally {
      outputStream.close();
    }
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), target,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
  }

  @Test
  public void testAppendToExistingFile() throws Throwable {
    byte[] original = dataset(8192, 'A', 'Z');
    byte[] appended = dataset(8192, '0', '9');
    createFile(getFileSystem(), target, false, original);
    FSDataOutputStream outputStream = getFileSystem().append(target);
      outputStream.write(appended);
      outputStream.close();
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), target,
                                                 original.length + appended.length);
    ContractTestUtils.validateFileContent(bytes, 
                                          new byte[] [] {
                                            original, appended
                                          }
                                          );
  }

  @Test
  public void testAppendMissingTarget() throws Throwable {
    try {
      FSDataOutputStream out = getFileSystem().append(target);
      //got here: trouble
      out.close();
      fail("expected a failure");
    } catch (Exception e) {
      //expected
      handleExpectedException(e);
    }
  }

  @Test
  public void testRenameFileBeingAppended() throws Throwable {
    touch(getFileSystem(), target);
    assertPathExists("original file does not exist", target);
    byte[] dataset = dataset(256, 'a', 'z');
    FSDataOutputStream outputStream = getFileSystem().append(target);
    outputStream.write(dataset);
    Path renamed = new Path(testPath, "renamed");
    outputStream.close();
    String listing = ls(testPath);
    

    //expected: the stream goes to the file that was being renamed, not
    //the original path
    assertPathDoesNotExist("Original filename still found after append:\n" +
                           listing, target);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), renamed,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
  }
}
