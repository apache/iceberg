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
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Test Metrics for ORC.
 */
public class TestOrcMetrics extends TestMetrics {

  static final ImmutableSet<Object> BINARY_TYPES = ImmutableSet.of(Type.TypeID.BINARY,
      Type.TypeID.FIXED, Type.TypeID.UUID);

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Override
  public FileFormat fileFormat() {
    return FileFormat.ORC;
  }

  @Override
  public Metrics getMetrics(InputFile file) {
    return OrcMetrics.fromInputFile(file);
  }

  @Override
  public InputFile writeRecordsWithSmallRowGroups(Schema schema, Record... records) throws IOException {
    throw new UnsupportedOperationException("supportsSmallRowGroups = " + supportsSmallRowGroups());
  }

  @Override
  public InputFile writeRecords(Schema schema, Record... records) throws IOException {
    return writeRecords(schema, ImmutableMap.of(), records);
  }

  private InputFile writeRecords(Schema schema, Map<String, String> properties, Record... records) throws IOException {
    File tmpFolder = temp.newFolder("orc");
    String filename = UUID.randomUUID().toString();
    OutputFile file = Files.localOutput(new File(tmpFolder, FileFormat.ORC.addExtension(filename)));
    try (FileAppender<Record> writer = ORC.write(file)
        .schema(schema)
        .setAll(properties)
        .createWriterFunc(GenericOrcWriter::buildWriter)
        .build()) {
      writer.addAll(Lists.newArrayList(records));
    }
    return file.toInputFile();
  }

  @Override
  public int splitCount(InputFile inputFile) throws IOException {
    return 0;
  }

  private boolean isBinaryType(Type type) {
    return BINARY_TYPES.contains(type.typeId());
  }

  @Override
  protected <T> void assertBounds(int fieldId, Type type, T lowerBound, T upperBound, Metrics metrics) {
    if (isBinaryType(type)) {
      Assert.assertFalse("ORC binary field should not have lower bounds.",
          metrics.lowerBounds().containsKey(fieldId));
      Assert.assertFalse("ORC binary field should not have upper bounds.",
          metrics.lowerBounds().containsKey(fieldId));
      return;
    }
    super.assertBounds(fieldId, type, lowerBound, upperBound, metrics);
  }
}
