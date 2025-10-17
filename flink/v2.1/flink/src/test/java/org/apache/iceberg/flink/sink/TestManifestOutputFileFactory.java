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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestManifestOutputFileFactory {

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION = new HadoopCatalogExtension("db", "table");

  private Table table;

  @BeforeEach
  void before() throws IOException {
    table = CATALOG_EXTENSION.catalog().createTable(TableIdentifier.of("table"), new Schema());
  }

  @Test
  public void testFileNameFormat() {
    String flinkJobId = "job123";
    String operatorUniqueId = "operator456";
    int subTaskId = 7;
    long attemptNumber = 2;
    long checkpointId = 100;
    Map<String, String> props = table.properties();

    ManifestOutputFileFactory factory =
        new ManifestOutputFileFactory(
            () -> table, props, flinkJobId, operatorUniqueId, subTaskId, attemptNumber);
    OutputFile file = factory.create(checkpointId);

    String filename = new File(file.location()).getName();
    assertThat(filename).startsWith("job123-operator456-00007-2-100-");
    assertThat(filename).endsWith(".avro");
  }

  @Test
  public void testUniqueFileNamesWithRecreatedFactory() {
    String flinkJobId = "test-job";
    String operatorUniqueId = "test-operator";
    int subTaskId = 0;
    long attemptNumber = 1;
    long checkpointId = 1;
    Map<String, String> props = table.properties();

    ManifestOutputFileFactory factory1 =
        new ManifestOutputFileFactory(
            () -> table, props, flinkJobId, operatorUniqueId, subTaskId, attemptNumber);
    OutputFile file1 = factory1.create(checkpointId);

    ManifestOutputFileFactory factory2 =
        new ManifestOutputFileFactory(
            () -> table, props, flinkJobId, operatorUniqueId, subTaskId, attemptNumber);
    OutputFile file2 = factory2.create(checkpointId);

    assertThat(file1.location()).isNotEqualTo(file2.location());
  }
}
