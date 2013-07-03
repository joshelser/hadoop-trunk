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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;

/**
 * Test Seek operations
 */
public abstract class AbstractSeekContractTest extends AbstractFSContractTestBase {
  protected static final Log LOG =
    LogFactory.getLog(AbstractSeekContractTest.class);
  public static final int SMALL_SEEK_FILE_LEN = 256;

  private Path testPath;
  private Path smallSeekFile;
  private Path zeroByteFile;
  private FSDataInputStream instream;
  static final int SWIFT_TEST_TIMEOUT = 0;

  @Override
  public void setup() throws Exception {
    super.setup();
    //delete the test directory
    testPath = path("test");
    smallSeekFile = new Path(testPath, "seekfile.txt");
    zeroByteFile = new Path(testPath, "zero.txt");
    byte[] block = dataset(SMALL_SEEK_FILE_LEN, 0, 255);
    //this file now has a simple rule: offset => value
    createFile(getFileSystem(),smallSeekFile, false, block);
    touch(getFileSystem(), zeroByteFile);
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.closeStream(instream);
    instream = null;
    cleanup("teardown", getFileSystem(), testPath);
    super.teardown();
  }

  @Test
  public void testSeekZeroByteFile() throws Throwable {
    instream = getFileSystem().open(zeroByteFile);
    assertEquals(0, instream.getPos());
    //expect initial read to fai;
    int result = instream.read();
    assertMinusOne("initial byte read", result);
    byte[] buffer = new byte[1];
    //expect that seek to 0 works
    instream.seek(0);
    //reread, expect same exception
    result = instream.read();
    assertMinusOne("post-seek byte read", result);
    result = instream.read(buffer, 0, 1);
    assertMinusOne("post-seek buffer read", result);
  }


  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testBlockReadZeroByteFile() throws Throwable {
    instream = getFileSystem().open(zeroByteFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    byte[] buffer = new byte[1];
    int result = instream.read(buffer, 0, 1);
    assertMinusOne("block read zero byte file", result);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekReadClosedFile() throws Throwable {
    instream = getFileSystem().open(smallSeekFile);
    instream.close();
    try {
      instream.seek(0);
      fail("seek succeeded on a closed stream");
    } catch (IOException e) {
      //expected a closed file
    }
    try {
      instream.read();
      fail("read succeeded on a closed stream");
    } catch (IOException e) {
      //expected a closed file
    }
    try {
      byte[] buffer = new byte[1];
      int result = instream.read(buffer, 0, 1);
      fail("read succeeded on a closed stream");
    } catch (IOException e) {
      //expected a closed file
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testNegativeSeek() throws Throwable {
    instream = getFileSystem().open(smallSeekFile);
    assertEquals(0, instream.getPos());
    try {
      instream.seek(-1);
      long p = instream.getPos();
      LOG.warn("Seek to -1 returned a position of " + p);
      int result = instream.read();
      fail(
        "expected an exception, got data " + result + " at a position of " + p);
    } catch (IOException e) {
      //bad seek -expected
    }
    assertEquals(0, instream.getPos());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekFile() throws Throwable {
    instream = getFileSystem().open(smallSeekFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    instream.seek(0);
    int result = instream.read();
    assertEquals(0, result);
    assertEquals(1, instream.read());
    assertEquals(2, instream.getPos());
    assertEquals(2, instream.read());
    assertEquals(3, instream.getPos());
    instream.seek(128);
    assertEquals(128, instream.getPos());
    assertEquals(128, instream.read());
    instream.seek(63);
    assertEquals(63, instream.read());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekAndReadPastEndOfFile() throws Throwable {
    instream = getFileSystem().open(smallSeekFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    //go just before the end
    instream.seek(SMALL_SEEK_FILE_LEN - 2);
    assertTrue("Premature EOF", instream.read() != -1);
    assertTrue("Premature EOF", instream.read() != -1);
    assertMinusOne("read past end of file", instream.read());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekAndPastEndOfFileThenReseekAndRead() throws Throwable {
    instream = getFileSystem().open(smallSeekFile);
    //go just before the end. This may or may not fail; it may be delayed until the
    //read
    try {
      instream.seek(SMALL_SEEK_FILE_LEN);
      //if this doesn't trigger, then read() is expected to fail
      assertMinusOne("read after seeking past EOF", instream.read());
    } catch (EOFException expected) {
      //here an exception was raised in seek
    }
    instream.seek(1);
    assertTrue("Premature EOF", instream.read() != -1);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testSeekBigFile() throws Throwable {
    Path testSeekFile = new Path(testPath, "bigseekfile.txt");
    byte[] block = dataset(65536, 0, 255);
    createFile(getFileSystem(), testSeekFile, false, block);
    instream = getFileSystem().open(testSeekFile);
    assertEquals(0, instream.getPos());
    //expect that seek to 0 works
    instream.seek(0);
    int result = instream.read();
    assertEquals(0, result);
    assertEquals(1, instream.read());
    assertEquals(2, instream.read());

    //do seek 32KB ahead
    instream.seek(32768);
    assertEquals("@32768", block[32768], (byte) instream.read());
    instream.seek(40000);
    assertEquals("@40000", block[40000], (byte) instream.read());
    instream.seek(8191);
    assertEquals("@8191", block[8191], (byte) instream.read());
    instream.seek(0);
    assertEquals("@0", 0, (byte) instream.read());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testPositionedBulkReadDoesntChangePosition() throws Throwable {
    Path testSeekFile = new Path(testPath, "bigseekfile.txt");
    byte[] block = dataset(65536, 0, 255);
    createFile(getFileSystem(), testSeekFile, false, block);
    instream = getFileSystem().open(testSeekFile);
    instream.seek(39999);
    assertTrue(-1 != instream.read());
    assertEquals(40000, instream.getPos());

    byte[] readBuffer = new byte[256];
    instream.read(128, readBuffer, 0, readBuffer.length);
    //have gone back
    assertEquals(40000, instream.getPos());
    //content is the same too
    assertEquals("@40000", block[40000], (byte) instream.read());
    //now verify the picked up data
    for (int i = 0; i < 256; i++) {
      assertEquals("@" + i, block[i + 128], readBuffer[i]);
    }
  }

  /**
   * Assert that the result value == -1; which implies
   * that a read was successful
   * @param text text to include in a message (usually the operation)
   * @param result read result to validate
   */
  protected void assertMinusOne(String text, int result) {
    assertEquals(text + " wrong read result " + result, -1, result);
  }
}
