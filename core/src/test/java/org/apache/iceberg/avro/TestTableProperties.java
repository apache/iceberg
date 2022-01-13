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

package org.apache.iceberg.avro;

import java.io.File;
import java.util.Map;
import org.apache.avro.file.DataFileConstants;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;

public class TestTableProperties {

  public static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get())
  );

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testAvroProperties() throws Exception {
    ImmutableMap<String, String> properties = ImmutableMap.of(
        TableProperties.AVRO_COMPRESSION, DataFileConstants.SNAPPY_CODEC,
        TableProperties.DEFAULT_FILE_FORMAT, FileFormat.AVRO.name());

    File folder = TEMPORARY_FOLDER.newFolder();

    String warehouse = folder.getAbsolutePath();
    String tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table path correctly.", new File(tablePath).mkdir());

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = new HadoopTables().create(SCHEMA, spec, properties, tablePath);

    Avro.WriteBuilder writeBuilder = Mockito.spy(Avro.write(Files.localOutput(TEMPORARY_FOLDER.newFile())));
    writeBuilder.forTable(table);
    ArgumentCaptor<Map<String, String>> argument = ArgumentCaptor.forClass(Map.class);
    verify(writeBuilder).setAll(argument.capture());
    Map<String, String> config = argument.getValue();

    Assert.assertEquals(DataFileConstants.SNAPPY_CODEC, config.get(TableProperties.AVRO_COMPRESSION));
    Assert.assertEquals(FileFormat.AVRO.name(), config.get(TableProperties.DEFAULT_FILE_FORMAT));
  }
}
