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
package org.apache.iceberg.data.orc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileFormatTestSupport;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcWritingTestUtils;
import org.apache.iceberg.orc.TestORCSchemaUtil;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class OrcFormat implements FileFormatTestSupport {
  @Override
  public FileFormat format() {
    return FileFormat.ORC;
  }

  @Override
  public void writeRecordsWithoutFieldIds(
      OutputFile outputFile, Schema schema, List<Record> records) throws IOException {
    TypeDescription typeWithIds = ORCSchemaUtil.convert(schema);
    TypeDescription typeWithoutIds = TestORCSchemaUtil.removeIds(typeWithIds);
    Path hadoopPath = new Path(outputFile.location());

    Configuration conf = new Configuration();
    OrcFile.WriterOptions options =
        OrcFile.writerOptions(conf)
            .useUTCTimestamp(true)
            .setSchema(typeWithoutIds)
            .fileSystem(OrcWritingTestUtils.outputFileSystem(outputFile));

    OrcRowWriter<Record> rowWriter = GenericOrcWriter.buildWriter(schema, typeWithIds);

    try (Writer orcWriter = OrcFile.createWriter(hadoopPath, options)) {
      VectorizedRowBatch batch = typeWithoutIds.createRowBatch();
      for (Record record : records) {
        rowWriter.write(record, batch);
        if (batch.size == batch.getMaxSize()) {
          orcWriter.addRowBatch(batch);
          batch.reset();
        }
      }

      if (batch.size > 0) {
        orcWriter.addRowBatch(batch);
        batch.reset();
      }
    }

    try (Reader reader = newOrcReader(outputFile.toInputFile(), conf)) {
      assertThat(TestORCSchemaUtil.hasIds(reader.getSchema())).isFalse();
    }
  }

  @Override
  public Map<String, String> testPropertiesToSet() {
    return Map.of(
        TableProperties.ORC_COMPRESSION, "snappy", OrcConf.ROW_INDEX_STRIDE.getAttribute(), "8192");
  }

  @Override
  public boolean checkTestProperties(InputFile inputFile) throws IOException {
    try (Reader reader = newOrcReader(inputFile, new Configuration())) {
      return "SNAPPY".equals(reader.getCompressionKind().name())
          && reader.getRowIndexStride() == 8192;
    }
  }

  @Override
  public String metadataValue(InputFile inputFile, String key) throws IOException {
    try (Reader reader = newOrcReader(inputFile, new Configuration())) {
      ByteBuffer metadataValue = reader.getMetadataValue(key).duplicate();
      byte[] bytes = new byte[metadataValue.remaining()];
      metadataValue.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  @Override
  public String splitSizeProperty() {
    return TableProperties.ORC_STRIPE_SIZE_BYTES;
  }

  private static Reader newOrcReader(InputFile inputFile, Configuration conf) throws IOException {
    Path hadoopPath = new Path(inputFile.location());
    OrcFile.ReaderOptions readerOptions =
        OrcFile.readerOptions(conf)
            .useUTCTimestamp(true)
            .filesystem(OrcWritingTestUtils.inputFileSystem(inputFile))
            .maxLength(inputFile.getLength());

    return OrcFile.createReader(hadoopPath, readerOptions);
  }
}
