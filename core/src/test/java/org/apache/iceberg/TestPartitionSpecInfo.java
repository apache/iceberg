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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionSpecInfo {

  @TempDir private Path temp;

  private final Schema schema =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
  private File tableDir = null;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @Parameter private int formatVersion;

  @BeforeEach
  public void setupTableDir() throws IOException {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
  }

  @AfterEach
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @TestTemplate
  public void testSpecIsUnpartitionedForVoidTranforms() {
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).alwaysNull("id").alwaysNull("data").build();

    assertThat(spec.isUnpartitioned()).isTrue();
  }

  @TestTemplate
  public void testSpecInfoUnpartitionedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    assertThat(spec.isUnpartitioned()).isTrue();
    assertThat(table.spec()).isEqualTo(spec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(spec.lastAssignedFieldId());
    assertThat(table.specs())
        .containsExactly(entry(spec.specId(), spec))
        .doesNotContainKey(Integer.MAX_VALUE);
  }

  @TestTemplate
  public void testSpecInfoPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("data").build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    assertThat(table.spec()).isEqualTo(spec);
    assertThat(table.spec().lastAssignedFieldId()).isEqualTo(spec.lastAssignedFieldId());
    assertThat(table.specs())
        .containsExactly(entry(spec.specId(), spec))
        .doesNotContainKey(Integer.MAX_VALUE);
  }

  @TestTemplate
  public void testColumnDropWithPartitionSpecEvolution() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    assertThat(table.spec()).isEqualTo(spec);

    TableMetadata base = TestTables.readMetadata("test");
    PartitionSpec newSpec =
        PartitionSpec.builderFor(table.schema()).identity("data").withSpecId(1).build();
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    int initialColSize = table.schema().columns().size();
    table.updateSchema().deleteColumn("id").commit();

    final Schema expectedSchema = new Schema(required(2, "data", Types.StringType.get()));

    assertThat(table.spec()).isEqualTo(newSpec);
    assertThat(table.specs())
        .containsExactly(entry(spec.specId(), spec), entry(newSpec.specId(), newSpec))
        .doesNotContainKey(Integer.MAX_VALUE);
    assertThat(table.schema().asStruct()).isEqualTo(expectedSchema.asStruct());
  }

  @TestTemplate
  public void testSpecInfoPartitionSpecEvolutionForV1Table() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 4).build();
    TestTables.TestTable table = TestTables.create(tableDir, "test", schema, spec, formatVersion);

    assertThat(table.spec()).isEqualTo(spec);

    TableMetadata base = TestTables.readMetadata("test");
    PartitionSpec newSpec =
        PartitionSpec.builderFor(table.schema()).bucket("data", 10).withSpecId(1).build();
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    assertThat(table.spec()).isEqualTo(newSpec);
    assertThat(table.specs())
        .containsExactly(entry(spec.specId(), spec), entry(newSpec.specId(), newSpec))
        .doesNotContainKey(Integer.MAX_VALUE);
  }
}
