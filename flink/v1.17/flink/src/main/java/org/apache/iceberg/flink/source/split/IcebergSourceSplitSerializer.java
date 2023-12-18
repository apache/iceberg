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

@Internal
public class IcebergSourceSplitSerializer implements SimpleVersionedSerializer<IcebergSourceSplit> {
  private static final int VERSION = 2;

  private final boolean caseSensitive;

  public IcebergSourceSplitSerializer(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(IcebergSourceSplit split) throws IOException {
    return split.serializeV2();
  }

  @Override
  public IcebergSourceSplit deserialize(int version, byte[] serialized) throws IOException {
    switch (version) {
      case 1:
        return IcebergSourceSplit.deserializeV1(serialized);
      case 2:
        return IcebergSourceSplit.deserializeV2(serialized, caseSensitive);
      default:
        throw new IOException(
            String.format(
                "Failed to deserialize IcebergSourceSplit. "
                    + "Encountered unsupported version: %d. Supported version are [1]",
                version));
    }
  }
}
