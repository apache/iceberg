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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.WriterImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBloomFilter {
  private static final Schema DATA_SCHEMA =
      new Schema(
          required(100, "id", Types.LongType.get()),
          required(101, "name", Types.StringType.get()),
          required(102, "price", Types.DoubleType.get()));

  @TempDir private File testFile;

  @Test
  public void testWriteOption() throws Exception {
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    OutputFile outFile = Files.localOutput(testFile);
    try (FileAppender<Record> writer =
        ORC.write(outFile)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(DATA_SCHEMA)
            .set("write.orc.bloom.filter.columns", "id,name")
            .set("write.orc.bloom.filter.fpp", "0.04")
            .build()) {

      Class<?> clazzOrcFileAppender = Class.forName("org.apache.iceberg.orc.OrcFileAppender");
      Field writerField = clazzOrcFileAppender.getDeclaredField("writer");
      writerField.setAccessible(true);
      WriterImpl orcWriter = (WriterImpl) writerField.get(writer);

      Class<?> clazzWriterImpl = Class.forName("org.apache.orc.impl.WriterImpl");
      Field bloomFilterColumnsField = clazzWriterImpl.getDeclaredField("bloomFilterColumns");
      Field bloomFilterFppField = clazzWriterImpl.getDeclaredField("bloomFilterFpp");
      bloomFilterColumnsField.setAccessible(true);
      bloomFilterFppField.setAccessible(true);
      boolean[] bloomFilterColumns = (boolean[]) bloomFilterColumnsField.get(orcWriter);
      double bloomFilterFpp = (double) bloomFilterFppField.get(orcWriter);

      // Validate whether the bloom filters are set in ORC SDK or not
      assertThat(bloomFilterColumns[1]).isTrue();
      assertThat(bloomFilterColumns[2]).isTrue();
      assertThat(bloomFilterFpp).isCloseTo(0.04, offset(1e-15));

      Record recordTemplate = GenericRecord.create(DATA_SCHEMA);
      Record record1 = recordTemplate.copy("id", 1L, "name", "foo", "price", 1.0);
      Record record2 = recordTemplate.copy("id", 2L, "name", "bar", "price", 2.0);
      writer.add(record1);
      writer.add(record2);
    }

    Class<?> clazzFileDump = Class.forName("org.apache.orc.tools.FileDump");
    Method getFormattedBloomFilters =
        clazzFileDump.getDeclaredMethod(
            "getFormattedBloomFilters",
            int.class,
            OrcIndex.class,
            OrcFile.WriterVersion.class,
            TypeDescription.Category.class,
            OrcProto.ColumnEncoding.class);
    getFormattedBloomFilters.setAccessible(true);

    try (Reader reader =
        OrcFile.createReader(
            new Path(outFile.location()), new OrcFile.ReaderOptions(new Configuration())); ) {
      boolean[] readCols = new boolean[] {false, true, true, false};
      RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
      OrcIndex indices = rows.readRowIndex(0, null, readCols);
      StripeInformation stripe = reader.getStripes().get(0);
      OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);

      String bloomFilterString =
          (String)
              getFormattedBloomFilters.invoke(
                  null,
                  1,
                  indices,
                  reader.getWriterVersion(),
                  reader.getSchema().findSubtype(1).getCategory(),
                  footer.getColumns(1));

      // Validate whether the bloom filters are written ORC files or not
      assertThat(bloomFilterString).contains("Bloom filters for column");
    }
  }

  @Test
  public void testInvalidFppOption() throws Exception {
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    assertThatThrownBy(
            () ->
                ORC.write(Files.localOutput(testFile))
                    .createWriterFunc(GenericOrcWriter::buildWriter)
                    .schema(DATA_SCHEMA)
                    .set("write.orc.bloom.filter.columns", "id,name")
                    .set("write.orc.bloom.filter.fpp", "-1")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bloom filter fpp must be > 0.0 and < 1.0");
  }
}
