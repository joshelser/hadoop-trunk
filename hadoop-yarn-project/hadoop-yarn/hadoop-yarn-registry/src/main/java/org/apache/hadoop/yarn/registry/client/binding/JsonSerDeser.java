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

package org.apache.hadoop.yarn.registry.client.binding;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Support for marshalling objects to and from JSON.
 * It constructs an object mapper as an instance field.
 * and synchronizes access to those methods
 * which use the mapper
 * @param <T>
 */
public class JsonSerDeser<T> {

  private static final Logger log = LoggerFactory.getLogger(JsonSerDeser.class);
  private static final String UTF_8 = "UTF-8";

  private final Class classType;
  private final ObjectMapper mapper;

  /**
   * Create an instance bound to a specific type
   * @param classType class to marshall
   */
  public JsonSerDeser(Class classType) {
    this.classType = classType;
    this.mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
        false);
  }

  /**
   * Convert from JSON
   * @param json input
   * @return the parsed JSON
   * @throws IOException IO
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public synchronized T fromJson(String json)
      throws IOException, JsonParseException, JsonMappingException {
    try {
      return (T) (mapper.readValue(json, classType));
    } catch (IOException e) {
      log.error("Exception while parsing json : " + e + "\n" + json, e);
      throw e;
    }
  }

  /**
   * Convert from a JSON file
   * @param jsonFile input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public synchronized T fromFile(File jsonFile)
      throws IOException, JsonParseException, JsonMappingException {
    try {
      return (T) (mapper.readValue(jsonFile, classType));
    } catch (IOException e) {
      log.error("Exception while parsing json file {}: {}", jsonFile, e);
      throw e;
    }
  }

  /**
   * Convert from a JSON file
   * @param resource input file
   * @return the parsed JSON
   * @throws IOException IO problems
   * @throws JsonMappingException failure to map from the JSON to this class
   */
  public synchronized T fromResource(String resource)
      throws IOException, JsonParseException, JsonMappingException {
    InputStream resStream = null;
    try {
      resStream = this.getClass().getResourceAsStream(resource);
      if (resStream == null) {
        throw new FileNotFoundException(resource);
      }
      return (T) (mapper.readValue(resStream, classType));
    } catch (IOException e) {
      log.error("Exception while parsing json resource {}: {}", resource, e);
      throw e;
    } finally {
      IOUtils.closeStream(resStream);
    }
  }

  /**
   * clone by converting to JSON and back again.
   * This is much less efficient than any Java clone process.
   * @param instance instance to duplicate
   * @return a new instance
   * @throws IOException problems.
   */
  public T fromInstance(T instance) throws IOException {
    return fromJson(toJson(instance));
  }

  /**
   * Deserialize from a byte array
   * @param b
   * @return
   * @throws IOException
   */
  public T fromBytes(byte[] b, int offset) throws IOException {
    int data = b.length - offset;
    if (data <= 0) {
      throw new EOFException("No data");
    }
    String json = new String(b, offset, data, UTF_8);
    return fromJson(json);
  }

  /**
   * Load from a Hadoop filesystem
   * @param fs filesystem
   * @param path path
   * @return a loaded CD
   * @throws IOException IO problems
   * @throws EOFException if not enough bytes were read in
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public T load(FileSystem fs, Path path)
      throws IOException, JsonParseException, JsonMappingException {
    FileStatus status = fs.getFileStatus(path);
    long len = status.getLen();
    byte[] b = new byte[(int) len];
    FSDataInputStream dataInputStream = fs.open(path);
    int count = dataInputStream.read(b);
    if (count != len) {
      throw new EOFException("Read finished prematurely");
    }
    return fromBytes(b, 0);
  }


  /**
   * Save a cluster description to a hadoop filesystem
   * @param fs filesystem
   * @param path path
   * @param overwrite should any existing file be overwritten
   * @throws IOException IO exception
   */
  public void save(FileSystem fs, Path path, T instance,
      boolean overwrite) throws
      IOException {
    FSDataOutputStream dataOutputStream = fs.create(path, overwrite);
    writeJsonAsBytes(instance, dataOutputStream);
  }

  /**
   * Write the json as bytes -then close the file
   * @param dataOutputStream an outout stream that will always be closed
   * @throws IOException on any failure
   */
  private void writeJsonAsBytes(T instance,
      DataOutputStream dataOutputStream) throws
      IOException {
    try {
      byte[] b = toBytes(instance);
      dataOutputStream.write(b);
    } finally {
      dataOutputStream.close();
    }
  }

  /**
   * Convert JSON To bytes
   * @param instance instance to convert
   * @return a byte array
   * @throws IOException
   */
  public byte[] toBytes(T instance) throws IOException {
    String json = toJson(instance);
    return json.getBytes(UTF_8);
  }


  /**
   * Convert an object to a JSON string
   * @param instance instance to convert
   * @return a JSON string description
   * @throws JsonParseException parse problems
   * @throws JsonMappingException O/J mapping problems
   */
  public synchronized String toJson(T instance) throws IOException,
      JsonGenerationException,
      JsonMappingException {
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    return mapper.writeValueAsString(instance);
  }
}
