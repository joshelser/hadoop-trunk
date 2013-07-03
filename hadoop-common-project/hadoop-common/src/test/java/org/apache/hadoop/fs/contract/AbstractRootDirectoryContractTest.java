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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * This class does things to the root directory.
 * Only subclass this for tests against transient filesystems where
 * you don't care about the data.
 */
public abstract class AbstractRootDirectoryContractTest extends AbstractFSContractTestBase {

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(ROOT_TESTS_ENABLED);
  }

  @Test
  public void testMkDirDepth1() throws Throwable {
    FileSystem fs = getFileSystem();
    Path dir = path("/testmkdirdepth1");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    ContractTestUtils.assertIsDirectory(getFileSystem(), dir);
    assertPathExists("directory already exists", dir);
    ContractTestUtils.assertDeleted(getFileSystem(), dir, true);
  }

  @Test
  public void testRmRootDirNonRecursive() throws Throwable {
    //extra sanity checks here to avoid support calls about complete loss of data
    skipIfUnsupported(ROOT_TESTS_ENABLED);
    Path dir = path("/");
    ContractTestUtils.assertIsDirectory(getFileSystem(), dir);
    assertFalse("rm / succeeded",getFileSystem().delete(dir, false));
    ContractTestUtils.assertIsDirectory(getFileSystem(), dir);
  }


  @Test
  public void testRmRootRecursive() throws Throwable {
    //extra sanity checks here to avoid support calls about complete loss of data
    skipIfUnsupported(ROOT_TESTS_ENABLED);
    Path dir = path("/");
    ContractTestUtils.assertIsDirectory(getFileSystem(), dir);
    Path file = path("/testRmRootRecursive");
    ContractTestUtils.touch(getFileSystem(), file);
    assertFalse("rm / succeeded", getFileSystem().delete(dir, true));
    assertPathDoesNotExist("expected file to be deleted",file);
    ContractTestUtils.assertIsDirectory(getFileSystem(), dir);
  }


}
