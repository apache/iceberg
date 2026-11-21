/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may obtain a copy of the License at
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
package org.apache.iceberg;

import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;

public class TestLegacyOrcManifestReader extends TestBase {
  @TestTemplate
  public void testPreviouslyWrittenOrcManifestRemainsReadable() throws IOException {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "parent", Types.StructType.of(optional(3, "leaf", Types.IntegerType.get()))));
    PartitionSpec spec = PartitionSpec.builderFor(schema).build();

    // This is the metadata emitted by the pre-fix ORC metrics conversion for a file with
    // two rows: parent = NULL, and parent = {leaf: 10}. No physical ORC data is needed
    // because the regression happens before the reader opens the file.
    Metrics legacyMetrics =
        new Metrics(2L, null, Map.of(1, 2L, 3, 1L), Map.of(1, 0L, 3, 0L), null);
    Metrics healthyMetrics =
        new Metrics(2L, null, Map.of(1, 2L, 3, 2L), Map.of(1, 0L, 3, 0L), null);
    DataFile legacy =
        DataFiles.builder(spec)
            .withPath("/path/to/legacy-nested-null.orc")
            .withFileSizeInBytes(100)
            .withMetrics(legacyMetrics)
            .build();
    DataFile healthy =
        DataFiles.builder(spec)
            .withPath("/path/to/healthy-nested.orc")
            .withFileSizeInBytes(100)
            .withMetrics(healthyMetrics)
            .build();
    DataFile parquet =
        DataFiles.builder(spec)
            .withPath("/path/to/healthy-nested.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(healthyMetrics)
            .build();

    OutputFile outputFile =
        table.ops()
            .io()
            .newOutputFile(
                temp.resolve(manifestFormat().addExtension("legacy-orc-m0")).toString());
    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, spec, outputFile, 1000L);
    try {
      writer.add(legacy);
      writer.add(healthy);
      writer.add(parquet);
    } finally {
      writer.close();
    }

    // Exercise the persisted Avro (V1-V3) or Parquet (V4) manifest, not an in-memory evaluator.
    ManifestFile manifest = writer.toManifestFile();
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, table.ops().io(), Map.of(spec.specId(), spec))
            .filterRows(isNull("parent.leaf"))) {
      assertThat(
              Streams.stream(reader)
                  .map(DataFile::location)
                  .collect(Collectors.toList()))
          .containsExactly(legacy.location());
    }
  }
}
