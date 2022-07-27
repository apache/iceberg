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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestMetrics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test Metrics for ORC. */
@RunWith(Parameterized.class)
public class TestOrcMetrics extends TestMetrics {

  static final ImmutableSet<Object> BINARY_TYPES =
      ImmutableSet.of(Type.TypeID.BINARY, Type.TypeID.FIXED, Type.TypeID.UUID);

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestOrcMetrics(int formatVersion) {
    super(formatVersion);
  }

  @Override
  protected OutputFile createOutputFile() throws IOException {
    File tmpFolder = temp.newFolder("orc");
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

  @Override
  protected <T> void assertBounds(
      int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    if (isBinaryType(type)) {
      Assert.assertFalse(
          "ORC binary field should not have lower bounds.",
          metrics.lowerBounds().containsKey(fieldId));
      Assert.assertFalse(
          "ORC binary field should not have upper bounds.",
          metrics.upperBounds().containsKey(fieldId));
      return;
    }
    super.assertBounds(fieldId, type, lowerBound, upperBound, metrics);
  }
}
