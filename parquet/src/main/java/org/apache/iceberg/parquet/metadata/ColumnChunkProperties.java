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
package org.apache.iceberg.parquet.metadata;

import java.util.Objects;
import java.util.Set;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;

public final class ColumnChunkProperties {
  private final ColumnPath path;
  private final PrimitiveType type;
  private final CompressionCodecName codec;
  private final Set<Encoding> encodings;

  public ColumnChunkProperties(
      ColumnPath path, PrimitiveType type, CompressionCodecName codec, Set<Encoding> encodings) {
    this.path = path;
    this.type = type;
    this.codec = codec;
    this.encodings = encodings;
  }

  public ColumnPath path() {
    return path;
  }

  public PrimitiveType type() {
    return type;
  }

  public CompressionCodecName codec() {
    return codec;
  }

  public Set<Encoding> encodings() {
    return encodings;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    ColumnChunkProperties that = (ColumnChunkProperties) obj;
    return Objects.equals(this.path, that.path)
        && Objects.equals(this.type, that.type)
        && Objects.equals(this.codec, that.codec)
        && Objects.equals(this.encodings, that.encodings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, type, codec, encodings);
  }

  @Override
  public String toString() {
    return "ColumnChunkProperties["
        + "path="
        + path
        + ", "
        + "type="
        + type
        + ", "
        + "codec="
        + codec
        + ", "
        + "encodings="
        + encodings
        + ']';
  }
}
