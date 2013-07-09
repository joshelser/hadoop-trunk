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

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

/**
 * Test creating files, overwrite options &c
 */
public abstract class AbstractRenameContractTest extends
                                                 AbstractFSContractTestBase {

  boolean rename(Path src, Path dst) throws IOException {
    return getFileSystem().rename(src, dst);
  }

  public void expectRenameToFault(Path src, Path dst) throws IOException {
    boolean renamed = rename(src, dst);
    //expected an exception
    String destDirLS = ContractTestUtils.ls(getFileSystem(), dst.getParent());
    getLog().error(
      "src dir " + ContractTestUtils.ls(getFileSystem(), src.getParent()));
    getLog().error("dest dir " + destDirLS);
    fail("expected rename(" + src + ", " + dst + " ) to fail," +
         " got a result of " + renamed
         + " and a dest dir of " + destDirLS);
  }

  @Test
  public void testRenameNewFileSameDir() throws Throwable {
    describe("rename a file into a new file in the same directory");
    Path path = path("testRenameNewFile");
    Path path2 = path("testRenameNewFile2");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, false);
    boolean rename = rename(path, path2);
    ContractTestUtils.verifyFileContents(getFileSystem(), path2, data);
    assertTrue("rename returned false though the contents were copied", rename);
  }

  @Test
  public void testRenameNoFileSameDir() throws Throwable {
    describe("rename a file into a new file in the same directory");
    Path path = path("testRenameNoFileSameDir");
    Path path2 = path("testRenameNoFileSameDir2");
    mkdirs(path.getParent());
    try {
      expectRenameToFault(path, path2);
      fail("rename a missing file unexpectedly succeeded");
    } catch (FileNotFoundException e) {
      handleExpectedException(e);
    }
  }

  @Test
  public void testRenameFileOverExistingFile() throws Throwable {
    describe("Verify renaming onto an existing file fails");
    Path path = path("testRenameFileOverExistingFile");
    Path path2 = path("testRenameFileOverExistingFile2");
    byte[] data = dataset(256, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024, false);
    byte[] data2 = dataset(10 * 1024, 'A', 'Z');
    writeDataset(getFileSystem(), path2, data2, data2.length, 1024, false);
    assertIsFile(path2);
    try {
      expectRenameToFault(path, path2);
    } catch (FileAlreadyExistsException e) {
      handleExpectedException(e);
    }
    ContractTestUtils.verifyFileContents(getFileSystem(), path2, data2);
  }

}
