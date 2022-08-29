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

package org.apache.iceberg.beam;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

public class IcebergIO {
  private IcebergIO() {
  }

  /** Write data to Hive. */
  public static Write write() {
    return new AutoValue_IcebergIO_Write.Builder()
            .setTableIdentifier(null)
            .setSchema(null).build();
  }

  /** Implementation of {@link Write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, POutput> {
    abstract @Nullable Map<String, String> getConfigProperties();

    abstract @Nullable TableIdentifier getTableIdentifier();

    abstract @Nullable Schema getSchema();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConfigProperties(Map<String, String> configProperties);

      abstract Builder<T> setTableIdentifier(TableIdentifier tableIdentifier);

      abstract Builder<T> setSchema(Schema schema);

      abstract Write<T> build();
    }

    public Write<T> withConfigProperties(Map<String, String> configProperties) {
      return toBuilder().setConfigProperties(configProperties).build();
    }

    public Write<T> withSchema(Schema schema) {
      return toBuilder().setSchema(schema).build();
    }

    public Write<T> withTableIdentifier(TableIdentifier tableIdentifier) {
      return toBuilder().setTableIdentifier(tableIdentifier).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument(getConfigProperties() != null, "withConfigProperties() is required");
      checkArgument(getTableIdentifier() != null, "withTableIdentifier() is required");
      // We take the filenames that are emitted by the FileWriter
      ParDo.SingleOutput<T, DataFile> fileWriter = ParDo.of(new FileWriter(this));
      PCollection<DataFile> dataFiles = input.apply("Write DataFiles", fileWriter)
              .setCoder(SerializableCoder.of(DataFile.class));

      // We use a combiner, to combine all the files to a single commit in
      // the Iceberg log
      final IcebergDataFileCommitter combiner = new IcebergDataFileCommitter(this);
      final Combine.Globally<DataFile, Snapshot> combined = Combine.globally(combiner).withoutDefaults();

      dataFiles.apply("Commit DataFiles", combined);
      return PDone.in(dataFiles.getPipeline());
    }
  }
}