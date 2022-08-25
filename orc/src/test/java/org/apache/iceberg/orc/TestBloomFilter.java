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

import java.io.File;
import java.lang.reflect.Field;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.apache.orc.impl.WriterImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestBloomFilter {
  private static final Schema DATA_SCHEMA =
      new Schema(
          required(100, "id", Types.LongType.get()),
          required(101, "name", Types.StringType.get()),
          required(102, "price", Types.DoubleType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testWriteOption() throws Exception {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(testFile))
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(DATA_SCHEMA)
            .set("write.orc.bloom.filter.columns", "id,name")
            .set("write.orc.bloom.filter.fpp", "0.04")
            .build()) {

      Class clazzOrcFileAppender = Class.forName("org.apache.iceberg.orc.OrcFileAppender");
      Field writerField = clazzOrcFileAppender.getDeclaredField("writer");
      writerField.setAccessible(true);
      WriterImpl orcWriter = (WriterImpl) writerField.get(writer);

      Class clazzWriterImpl = Class.forName("org.apache.orc.impl.WriterImpl");
      Field bloomFilterColumnsField = clazzWriterImpl.getDeclaredField("bloomFilterColumns");
      Field bloomFilterFppField = clazzWriterImpl.getDeclaredField("bloomFilterFpp");
      bloomFilterColumnsField.setAccessible(true);
      bloomFilterFppField.setAccessible(true);
      boolean[] bloomFilterColumns = (boolean[]) bloomFilterColumnsField.get(orcWriter);
      double bloomFilterFpp = (double) bloomFilterFppField.get(orcWriter);

      Assert.assertTrue(bloomFilterColumns[1]);
      Assert.assertTrue(bloomFilterColumns[2]);
      Assert.assertEquals(0.04, bloomFilterFpp, 1e-15);
    }
  }

  @Test
  public void testInvalidFppOption() throws Exception {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(testFile))
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .schema(DATA_SCHEMA)
            .set("write.orc.bloom.filter.columns", "id,name")
            .set("write.orc.bloom.filter.fpp", "-1")
            .build()) {
      Assert.fail("Expected exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Bloom filter fpp must be > 0.0 and < 1.0"));
    }
  }
}
