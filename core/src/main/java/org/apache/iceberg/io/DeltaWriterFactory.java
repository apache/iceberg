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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Factory to create {@link DeltaWriter}, which have few dependencies factories to create different kinds of writers.
 */
public interface DeltaWriterFactory<T> {

  /**
   * Create a factory to initialize the {@link DeltaWriter}.
   */
  DeltaWriter<T> createDeltaWriter(PartitionKey partitionKey, Context context);

  /**
   * Create a factory to initialize the {@link FileAppender}.
   */
  FileAppenderFactory<T> createFileAppenderFactory();

  /**
   * Create a factory to initialize the {@link org.apache.iceberg.DataFileWriter}.
   */
  ContentFileWriterFactory<DataFile, T> createDataFileWriterFactory();

  /**
   * Create a factory to initialize the {@link org.apache.iceberg.deletes.EqualityDeleteWriter}.
   */
  ContentFileWriterFactory<DeleteFile, T> createEqualityDeleteWriterFactory(List<Integer> equalityFieldIds,
                                                                            Schema rowSchema);

  /**
   * Create a factory to initialize the {@link org.apache.iceberg.deletes.PositionDeleteWriter}.
   */
  ContentFileWriterFactory<DeleteFile, PositionDelete<T>> createPosDeleteWriterFactory(Schema rowSchema);

  class Context {
    private final boolean allowPosDelete;
    private final boolean allowEqualityDelete;
    private final List<Integer> equalityFieldIds;
    private final Schema posDeleteRowSchema;
    private final Schema eqDeleteRowSchema;

    private Context(boolean allowPosDelete, boolean allowEqualityDelete,
                    List<Integer> equalityFieldIds, Schema eqDeleteRowSchema,
                    Schema posDeleteRowSchema) {
      this.allowPosDelete = allowPosDelete;
      this.allowEqualityDelete = allowEqualityDelete;
      this.equalityFieldIds = equalityFieldIds;
      this.eqDeleteRowSchema = eqDeleteRowSchema;
      this.posDeleteRowSchema = posDeleteRowSchema;
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

    public Schema posDeleteRowSchema() {
      return posDeleteRowSchema;
    }

    public Schema eqDeleteRowSchema() {
      return eqDeleteRowSchema;
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private boolean allowPosDelete = false;
      private boolean allowEqualityDelete = false;
      private List<Integer> equalityFieldIds = ImmutableList.of();
      private Schema eqDeleteRowSchema;
      private Schema posDeleteRowSchema;

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

      public Builder eqDeleteRowSchema(Schema newRowSchema) {
        this.eqDeleteRowSchema = newRowSchema;
        return this;
      }

      public Builder posDeleteRowSchema(Schema newRowSchema) {
        this.posDeleteRowSchema = newRowSchema;
        return this;
      }

      public Context build() {
        if (allowEqualityDelete) {
          Preconditions.checkNotNull(equalityFieldIds, "Equality field ids shouldn't be null for equality deletes");
          Preconditions.checkNotNull(eqDeleteRowSchema, "Row schema shouldn't be null for equality deletes");

          for (Integer fieldId : equalityFieldIds) {
            Preconditions.checkNotNull(eqDeleteRowSchema.findField(fieldId),
                "Unknown field with id %s in provided equality row schema: %s", fieldId, eqDeleteRowSchema);
          }
        }

        return new Context(allowPosDelete, allowEqualityDelete, equalityFieldIds, eqDeleteRowSchema,
            posDeleteRowSchema);
      }
    }
  }
}
