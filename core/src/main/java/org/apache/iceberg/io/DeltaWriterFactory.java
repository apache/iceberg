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

package org.apache.iceberg.io;

import java.util.List;
import org.apache.iceberg.ContentFileWriterFactory;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public interface DeltaWriterFactory<T> {

  DeltaWriter<T> createDeltaWriter(PartitionKey partitionKey, Context context);

  FileAppenderFactory<T> createFileAppenderFactory();

  ContentFileWriterFactory<DataFile, T> createDataFileWriterFactory();

  ContentFileWriterFactory<DeleteFile, T> createEqualityDeleteWriterFactory(List<Integer> equalityFieldIds,
                                                                            Schema rowSchema);

  ContentFileWriterFactory<DeleteFile, PositionDelete<T>> createPosDeleteWriterFactory();

  class Context {
    private final boolean allowPosDelete;
    private final boolean allowEqualityDelete;
    private final List<Integer> equalityFieldIds;
    private final Schema rowSchema;

    private Context(boolean allowPosDelete, boolean allowEqualityDelete, List<Integer> equalityFieldIds,
                    Schema rowSchema) {
      this.allowPosDelete = allowPosDelete;
      this.allowEqualityDelete = allowEqualityDelete;
      this.equalityFieldIds = equalityFieldIds;
      this.rowSchema = rowSchema;
    }

    public boolean allowPosDelete() {
      return allowPosDelete;
    }

    public boolean allowEqualityDelete() {
      return allowEqualityDelete;
    }

    public List<Integer> equalityFieldIds() {
      return equalityFieldIds;
    }

    public Schema rowSchema() {
      return rowSchema;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private boolean allowPosDelete = false;
      private boolean allowEqualityDelete = false;
      private List<Integer> equalityFieldIds = ImmutableList.of();
      private Schema rowSchema;

      public Builder allowPosDelete(boolean enable) {
        this.allowPosDelete = enable;
        return this;
      }

      public Builder allowEqualityDelete(boolean enable) {
        this.allowEqualityDelete = enable;
        return this;
      }

      public Builder equalityFieldIds(List<Integer> newEqualityFieldIds) {
        this.equalityFieldIds = ImmutableList.copyOf(newEqualityFieldIds);
        return this;
      }

      public Builder rowSchema(Schema newRowSchema) {
        this.rowSchema = newRowSchema;
        return this;
      }

      public Context build() {
        return new Context(allowPosDelete, allowEqualityDelete, equalityFieldIds, rowSchema);
      }
    }
  }
}
