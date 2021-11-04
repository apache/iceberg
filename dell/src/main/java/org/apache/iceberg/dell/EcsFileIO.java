/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.dell;

import com.emc.object.s3.S3Client;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link java.io.Externalizable} FileIO of ECS S3 object client.
 */
public class EcsFileIO implements FileIO, Externalizable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(EcsFileIO.class);

  /**
   * Saved properties for {@link java.io.Serializable}
   */
  private Map<String, String> properties;
  private S3Client client;

  /**
   * Blank constructor
   */
  public EcsFileIO() {
  }

  @Override
  public void initialize(Map<String, String> inputProperties) {
    this.properties = ImmutableMap.copyOf(inputProperties);
    this.client = EcsClientFactory.create(inputProperties);
  }

  @Override
  public InputFile newInputFile(String path) {
    return new EcsInputFile(client, path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new EcsOutputFile(client, path);
  }

  @Override
  public void deleteFile(String path) {
    EcsURI uri = EcsURI.create(path);
    client.deleteObject(uri.getBucket(), uri.getName());
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(properties);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    @SuppressWarnings("unchecked")
    Map<String, String> inputProperties = (Map<String, String>) in.readObject();
    initialize(inputProperties);
  }

  @Override
  public void close() {
    client.destroy();
    log.info("FileIO closed");
  }

  @VisibleForTesting
  Map<String, String> getProperties() {
    return properties;
  }

  @VisibleForTesting
  S3Client getClient() {
    return client;
  }
}
