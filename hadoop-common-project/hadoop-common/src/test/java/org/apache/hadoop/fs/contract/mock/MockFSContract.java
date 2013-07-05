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

package org.apache.hadoop.fs.contract.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;

import java.io.IOException;

/**
 * mock fs for testing the contract tests
 */
public class MockFSContract extends AbstractFSContract {

  public MockFSContract(Configuration conf) {
    this(conf, true);
  }

  public MockFSContract(Configuration conf, boolean enabled) {
    super(conf);
    setEnabled(enabled);
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return FileSystem.getLocal(getConf());
  }

  @Override
  public String getScheme() {
    return "mock";
  }

  @Override
  public Path getTestPath() {
    return new Path("/test");
  }
}
