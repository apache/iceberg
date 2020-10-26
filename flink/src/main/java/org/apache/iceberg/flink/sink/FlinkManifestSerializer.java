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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class FlinkManifestSerializer implements SimpleVersionedSerializer<ManifestFile> {
  private static final int VERSION_NUM = 1;
  static final FlinkManifestSerializer INSTANCE = new FlinkManifestSerializer();

  @Override
  public int getVersion() {
    return VERSION_NUM;
  }

  @Override
  public byte[] serialize(ManifestFile manifestFile) throws IOException {
    Preconditions.checkNotNull(manifestFile, "ManifestFile to be serialized should not be null");

    return ManifestFiles.encode(manifestFile);
  }

  @Override
  public ManifestFile deserialize(int version, byte[] serialized) throws IOException {
    return ManifestFiles.decode(serialized);
  }
}
