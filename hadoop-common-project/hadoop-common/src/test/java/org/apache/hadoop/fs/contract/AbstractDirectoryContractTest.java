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

import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;

/**
 * Test directory operations
 */
public abstract class AbstractDirectoryContractTest extends AbstractFSContractTestBase {

  @Test
  public void testMkDirRmDir() throws Throwable {
    FileSystem fs = getFileSystem();
    
    Path dir = path("testMkDirRmDir");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    assertPathExists("mkdir failed", dir);
    assertDeleted(dir, false);
  }

  @Test
  public void testMkDirRmRfDir() throws Throwable {
    describe("create a directory then recursive delete it");
    FileSystem fs = getFileSystem();
    Path dir = path("testMkDirRmRfDir");
    assertPathDoesNotExist("directory already exists", dir);
    fs.mkdirs(dir);
    assertPathExists("mkdir failed", dir);
    assertDeleted(dir, true);
  }

  @Test
  public void testNoMkdirOverFile() throws Throwable {
    describe("try to mkdir over a file");
    FileSystem fs = getFileSystem();
    Path path = path("testNoMkdirOverFile");
    byte[] dataset = dataset(1024, ' ', 'z');
    createFile(getFileSystem(), path, false, dataset);
    try {
      boolean made = fs.mkdirs(path);
      assertFalse("mkdirs succeeded over a file" + ls(path), made);
    } catch (IOException e) {
      //here the FS says "no create"  
    }
    assertIsFile(path);
    byte[] bytes = ContractTestUtils.readDataset(getFileSystem(), path,
                                                 dataset.length);
    ContractTestUtils.compareByteArrays(dataset, bytes, dataset.length);
    assertPathExists("mkdir failed", path);
    assertDeleted(path, true);
  }
}
