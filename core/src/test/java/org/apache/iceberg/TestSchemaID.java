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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSchemaID extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testNoChange() {
    int onlyId = table.schema().schemaId();
    Map<Integer, Schema> onlySchemaMap = schemaMap(table.schema());

    // add files to table
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    TestHelpers.assertSameSchemaMap(onlySchemaMap, table.schemas());
    assertThat(table.currentSnapshot().schemaId()).isEqualTo(table.schema().schemaId());

    assertThat(table.snapshots()).extracting(Snapshot::schemaId).containsExactly(onlyId);

    // remove file from table
    table.newDelete().deleteFile(FILE_A).commit();

    TestHelpers.assertSameSchemaMap(onlySchemaMap, table.schemas());
    assertThat(table.currentSnapshot().schemaId()).isEqualTo(table.schema().schemaId());

    assertThat(table.snapshots()).extracting(Snapshot::schemaId).containsExactly(onlyId, onlyId);

    // add file to table
    table.newFastAppend().appendFile(FILE_A2).commit();

    TestHelpers.assertSameSchemaMap(onlySchemaMap, table.schemas());
    assertThat(table.currentSnapshot().schemaId()).isEqualTo(table.schema().schemaId());

    assertThat(table.snapshots())
        .extracting(Snapshot::schemaId)
        .containsExactly(onlyId, onlyId, onlyId);
  }

  @TestTemplate
  public void testSchemaIdChangeInSchemaUpdate() {
    Schema originalSchema = table.schema();

    // add files to table
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    TestHelpers.assertSameSchemaMap(schemaMap(table.schema()), table.schemas());
    assertThat(table.currentSnapshot().schemaId()).isEqualTo(table.schema().schemaId());

    assertThat(table.snapshots())
        .extracting(Snapshot::schemaId)
        .containsExactly(originalSchema.schemaId());

    // update schema
    table.updateSchema().addColumn("data2", Types.StringType.get()).commit();

    Schema updatedSchema =
        new Schema(
            1,
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()),
            optional(3, "data2", Types.StringType.get()));

    TestHelpers.assertSameSchemaMap(schemaMap(originalSchema, updatedSchema), table.schemas());
    assertThat(table.currentSnapshot().schemaId())
        .as(
            "Current snapshot's schemaId should be old since update schema doesn't create new snapshot")
        .isEqualTo(originalSchema.schemaId());
    assertThat(table.schema().asStruct()).isEqualTo(updatedSchema.asStruct());

    assertThat(table.snapshots())
        .extracting(Snapshot::schemaId)
        .containsExactly(originalSchema.schemaId());

    // remove file from table
    table.newDelete().deleteFile(FILE_A).commit();

    TestHelpers.assertSameSchemaMap(schemaMap(originalSchema, updatedSchema), table.schemas());
    assertThat(table.currentSnapshot().schemaId()).isEqualTo(updatedSchema.schemaId());
    assertThat(table.schema().asStruct()).isEqualTo(updatedSchema.asStruct());

    assertThat(table.snapshots())
        .extracting(Snapshot::schemaId)
        .containsExactly(originalSchema.schemaId(), updatedSchema.schemaId());
    // add files to table
    table.newAppend().appendFile(FILE_A2).commit();

    TestHelpers.assertSameSchemaMap(schemaMap(originalSchema, updatedSchema), table.schemas());
    assertThat(table.currentSnapshot().schemaId()).isEqualTo(updatedSchema.schemaId());
    assertThat(table.schema().asStruct()).isEqualTo(updatedSchema.asStruct());

    assertThat(table.snapshots())
        .extracting(Snapshot::schemaId)
        .containsExactly(
            originalSchema.schemaId(), updatedSchema.schemaId(), updatedSchema.schemaId());
  }

  private Map<Integer, Schema> schemaMap(Schema... schemas) {
    return Arrays.stream(schemas).collect(Collectors.toMap(Schema::schemaId, Function.identity()));
  }
}
