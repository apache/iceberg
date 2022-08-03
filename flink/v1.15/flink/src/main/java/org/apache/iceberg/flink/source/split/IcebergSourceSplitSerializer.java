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
package org.apache.iceberg.flink.source.split;

import java.io.IOException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * TODO: use Java serialization for now. Will switch to more stable serializer from <a
 * href="https://github.com/apache/iceberg/issues/1698">issue-1698</a>.
 */
@Internal
public class IcebergSourceSplitSerializer implements SimpleVersionedSerializer<IcebergSourceSplit> {
  public static final IcebergSourceSplitSerializer INSTANCE = new IcebergSourceSplitSerializer();
  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergSourceSplit split) throws IOException {
    return split.serializeV1();
  }

  @Override
  public IcebergSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return IcebergSourceSplit.deserializeV1(serialized);
      default:
        throw new IOException(
            String.format(
                "Failed to deserialize IcebergSourceSplit. "
                    + "Encountered unsupported version: %d. Supported version are [1]",
                version));
    }
  }
}
