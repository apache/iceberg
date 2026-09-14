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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestMetrics;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/** Test Metrics for ORC. */
@ExtendWith(ParameterizedTestExtension.class)
public class TestOrcMetrics extends TestMetrics {

  static final ImmutableSet<Object> BINARY_TYPES =
      ImmutableSet.of(Type.TypeID.BINARY, Type.TypeID.FIXED, Type.TypeID.UUID);

  @Override
  protected OutputFile createOutputFile() throws IOException {
    File tmpFolder = java.nio.file.Files.createTempDirectory(temp, "orc").toFile();
    String filename = UUID.randomUUID().toString();
    return Files.localOutput(new File(tmpFolder, FileFormat.ORC.addExtension(filename)));
  }

  @Override
  public FileFormat fileFormat() {
    return FileFormat.ORC;
  }

  @Override
  public Metrics getMetrics(Schema schema, Record... records) throws IOException {
    return getMetrics(schema, MetricsConfig.getDefault(), records);
  }

  @Override
  public Metrics getMetrics(Schema schema, MetricsConfig metricsConfig, Record... records)
      throws IOException {
    return getMetrics(schema, createOutputFile(), ImmutableMap.of(), metricsConfig, records);
  }

  @Override
  protected Metrics getMetricsForRecordsWithSmallRowGroups(
      Schema schema, OutputFile outputFile, Record... records) {
    throw new UnsupportedOperationException("supportsSmallRowGroups = " + supportsSmallRowGroups());
  }

  private Metrics getMetrics(
      Schema schema,
      OutputFile file,
      Map<String, String> properties,
      MetricsConfig metricsConfig,
      Record... records)
      throws IOException {
    FileAppender<Record> writer =
        ORC.write(file)
            .schema(schema)
            .setAll(properties)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .metricsConfig(metricsConfig)
            .build();
    try (FileAppender<Record> appender = writer) {
      appender.addAll(Lists.newArrayList(records));
    }
    return writer.metrics();
  }

  @Override
  public int splitCount(InputFile inputFile) {
    return 0;
  }

  private boolean isBinaryType(Type type) {
    return BINARY_TYPES.contains(type.typeId());
  }

  @TestTemplate
  public void timestampNanoBoundsKeepNanoPrecision() throws IOException {
    Types.TimestampNanoType nanoType = Types.TimestampNanoType.withoutZone();
    Schema schema = new Schema(Types.NestedField.optional(1, "tsNano", nanoType));

    // sub-microsecond nanos that would change if truncated to micros
    LocalDateTime lower = LocalDateTime.parse("1970-01-01T00:00:00.000001500");
    LocalDateTime upper = LocalDateTime.parse("2024-01-02T03:04:05.123456789");

    GenericRecord lowerRecord = GenericRecord.create(schema);
    lowerRecord.set(0, lower);
    GenericRecord upperRecord = GenericRecord.create(schema);
    upperRecord.set(0, upper);

    Metrics metrics = getMetrics(schema, lowerRecord, upperRecord);

    LocalDateTime epoch = LocalDateTime.parse("1970-01-01T00:00:00");
    long expectedLower = ChronoUnit.NANOS.between(epoch, lower);
    long expectedUpper = ChronoUnit.NANOS.between(epoch, upper);

    long actualLower = Conversions.fromByteBuffer(nanoType, metrics.lowerBounds().get(1));
    long actualUpper = Conversions.fromByteBuffer(nanoType, metrics.upperBounds().get(1));

    assertThat(actualLower).isEqualTo(expectedLower);
    assertThat(actualUpper).isEqualTo(expectedUpper);
    // guard against the micros regression: the bound must not be ~1000x smaller
    assertThat(actualLower).isEqualTo(1500L);
  }

  @Override
  protected <T> void assertBounds(
      int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    if (isBinaryType(type)) {
      assertThat(metrics.lowerBounds()).doesNotContainKey(fieldId);
      assertThat(metrics.upperBounds()).doesNotContainKey(fieldId);
      return;
    }
    super.assertBounds(fieldId, type, lowerBound, upperBound, metrics);
  }
}
