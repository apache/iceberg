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
package org.apache.iceberg.flink.sink.writer;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

public class StreamWriterStateSerializer implements SimpleVersionedSerializer<StreamWriterState> {
  private static final int VERSION_1 = 1;

  @Override
  public int getVersion() {
    return VERSION_1;
  }

  @Override
  public byte[] serialize(StreamWriterState streamWriterState) throws IOException {
    return InstantiationUtil.serializeObject(streamWriterState);
  }

  @Override
  public StreamWriterState deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case VERSION_1:
        try {
          return InstantiationUtil.deserializeObject(
              serialized, StreamWriterState.class.getClassLoader());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Failed to deserialize the IcebergStreamWriterState.", e);
        }
      default:
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }
  }
}
