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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestOutputFileFactory extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  private static final int PARTITION_ID = 1;
  private static final int TASK_ID = 100;

  @TestTemplate
  public void testOutputFileFactoryWithCustomFormat() {
    table.updateProperties().defaultFormat(FileFormat.ORC).commit();

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, PARTITION_ID, TASK_ID).format(FileFormat.AVRO).build();

    String location = fileFactory.newOutputFile().encryptingOutputFile().location();
    assertThat(FileFormat.fromFileName(location)).isEqualTo(FileFormat.AVRO);
  }

  @TestTemplate
  public void testOutputFileFactoryWithMultipleSpecs() {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, PARTITION_ID, TASK_ID).operationId("append").build();

    EncryptedOutputFile unpartitionedFile =
        fileFactory.newOutputFile(PartitionSpec.unpartitioned(), null);
    String unpartitionedFileLocation = unpartitionedFile.encryptingOutputFile().location();
    assertThat(unpartitionedFileLocation).endsWith("data/00001-100-append-00001.parquet");

    Record record = GenericRecord.create(table.schema()).copy(ImmutableMap.of("data", "aaa"));
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(record);
    EncryptedOutputFile partitionedFile = fileFactory.newOutputFile(table.spec(), partitionKey);
    String partitionedFileLocation = partitionedFile.encryptingOutputFile().location();
    assertThat(partitionedFileLocation).endsWith("data_bucket=7/00001-100-append-00002.parquet");
  }

  @TestTemplate
  public void testWithCustomSuffix() {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, PARTITION_ID, TASK_ID)
            .operationId("append")
            .suffix("suffix")
            .build();

    EncryptedOutputFile unpartitionedFile =
        fileFactory.newOutputFile(PartitionSpec.unpartitioned(), null);
    String unpartitionedFileLocation = unpartitionedFile.encryptingOutputFile().location();
    assertThat(unpartitionedFileLocation).endsWith("data/00001-100-append-00001-suffix.parquet");

    Record record = GenericRecord.create(table.schema()).copy(ImmutableMap.of("data", "aaa"));
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    partitionKey.partition(record);
    EncryptedOutputFile partitionedFile = fileFactory.newOutputFile(table.spec(), partitionKey);
    String partitionedFileLocation = partitionedFile.encryptingOutputFile().location();
    assertThat(partitionedFileLocation)
        .endsWith("data_bucket=7/00001-100-append-00002-suffix.parquet");
  }
}
