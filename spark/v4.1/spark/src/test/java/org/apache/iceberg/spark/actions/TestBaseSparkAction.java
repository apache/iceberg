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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestBaseSparkAction extends TestBase {
  private static final HadoopTables TABLES = new HadoopTables();

  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));

  @TempDir private Path temp;

  @Test
  void rejectsNullMetadataLocation() throws IOException {
    File tableLocation = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableLocation.delete()).isTrue();

    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), tableLocation.toString());
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata metadata = spy(ops.current());
    assertThat(metadata.metadataFileLocation()).isNotNull();

    doReturn(null).when(metadata).metadataFileLocation();
    assertThat(metadata.metadataFileLocation()).isNull();

    TestAction action = new TestAction();
    assertThatThrownBy(() -> action.callNewStaticTable(metadata, table.io()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("metadata file location is null");
  }

  private static class TestAction extends BaseSparkAction<TestAction> {
    TestAction() {
      super(spark);
    }

    Table callNewStaticTable(TableMetadata metadata, FileIO io) {
      return newStaticTable(metadata, io);
    }

    @Override
    protected TestAction self() {
      return null;
    }
  }
}
