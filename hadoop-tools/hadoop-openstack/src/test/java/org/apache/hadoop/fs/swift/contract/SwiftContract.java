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

package org.apache.hadoop.fs.swift.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The contract of S3N: only enabled if the test bucket is provided
 */
public class SwiftContract extends AbstractFSContract {

  public static final String CONTRACT_XML = "contract/swift.xml";
  /**
   *
   */
  public static final String TEST_FS_NAME = "test.fs.name";

  private String fsName;
  private URI fsURI;
  private FileSystem fs;

  public SwiftContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
  }

  @Override
  public void init() throws IOException {
    super.init();
    //this test is only enabled if the test FS is present
    fsName = getOption(TEST_FS_NAME, null);
    boolean enabled = fsName != null
                      && !fsName.isEmpty()
                      && !fsName.equals("swift:///");
    setEnabled(enabled);
    if (enabled) {
      try {
        fsURI = new URI(fsName);
        fs = FileSystem.get(fsURI, getConf());
      } catch (URISyntaxException e) {
        throw new IOException("Invalid URI " + fsName
                              + " for config option " + TEST_FS_NAME);
      } catch (IllegalArgumentException e) {
        throw new IOException("Invalid S3N URI " + fsName
                              + " for config option " + TEST_FS_NAME, e);
      }
    }
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return fs;
  }

  @Override
  public String getScheme() {
    return SwiftNativeFileSystem.SWIFT;
  }

  @Override
  public Path getTestPath() {
    Path path = new Path("/test");
    return path;
  }

  @Override
  public String toString() {
    return "Swift Contract against " + fsName;
  }
}
