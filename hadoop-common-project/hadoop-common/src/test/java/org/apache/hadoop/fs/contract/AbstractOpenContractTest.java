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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;

/**
 * Test Seek operations
 */
public abstract class AbstractOpenContractTest extends AbstractFSContractTestBase {

  private FSDataInputStream instream;

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setInt(CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY, 4096);
    return conf;
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.closeStream(instream);
    instream = null;
    super.teardown();
  }

  @Test
  public void testOpenReadZeroByteFile() throws Throwable {
    describe("create & read a 0 byte file");
    Path path = path("zero.txt");
    touch(getFileSystem(), path);
    instream = getFileSystem().open(path);
    assertEquals(0, instream.getPos());
    //expect initial read to fai;
    int result = instream.read();
    assertMinusOne("initial byte read", result);
  }

  @Test
  public void testOpenReadDir() throws Throwable {
    describe("create & read a directory");
    Path path = path("zero.dir");
    mkdirs(path);
    try {
      instream = getFileSystem().open(path);
      //at this point we've opened a directory
      fail("A directory has been opened for reading");
    } catch (FileNotFoundException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("opening a directory for reading",
                             "FileNotFoundException",
                             e);
    }
  }

  @Test
  public void testOpenReadDirWithChild() throws Throwable {
    describe("create & read a directory which has a child");
    Path path = path("zero.dir");
    mkdirs(path);
    Path path2 = new Path(path, "child");
    mkdirs(path2);

    try {
      instream = getFileSystem().open(path);
      //at this point we've opened a directory
      fail("A directory has been opened for reading");
    } catch (FileNotFoundException e) {
      handleExpectedException(e);
    } catch (IOException e) {
      handleRelaxedException("opening a directory for reading",
                             "FileNotFoundException",
                             e);
    }
  }

  @Test
  public void testOpenFileTwice() throws Throwable {
    describe("verify that two opened file streams are independent");
    Path path = path("seekfile.txt");
    byte[] block = dataset(TEST_FILE_LEN, 0, 255);
    //this file now has a simple rule: offset => value
    createFile(getFileSystem(), path, false, block);
    //open first
    FSDataInputStream instream1 = getFileSystem().open(path);
    int c = instream1.read();
    assertEquals(0,c);
    FSDataInputStream instream2 = null;
    try {
      instream2 = getFileSystem().open(path);
      assertEquals("first read of instream 2", 0, instream2.read());
      assertEquals("second read of instream 1", 1, instream1.read());
      instream1.close();
      assertEquals("second read of instream 2", 1, instream2.read());
    } finally {
      IOUtils.closeStream(instream2);
    }

  }


}
