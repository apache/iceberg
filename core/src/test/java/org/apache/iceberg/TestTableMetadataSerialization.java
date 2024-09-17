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

import static org.apache.iceberg.TestHelpers.assertSameSchemaList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTableMetadataSerialization extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testSerialization() throws Exception {
    // add a commit to the metadata so there is at least one snapshot, and history
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    TableMetadata meta = table.ops().current();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ObjectOutputStream writer = new ObjectOutputStream(out)) {
      writer.writeObject(meta);
    }

    TableMetadata result;
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ObjectInputStream reader = new ObjectInputStream(in)) {
      result = (TableMetadata) reader.readObject();
    }

    assertThat(result.metadataFileLocation()).isEqualTo(meta.metadataFileLocation());
    assertThat(result.uuid()).isEqualTo(meta.uuid());
    assertThat(result.location()).isEqualTo(meta.location());
    assertThat(result.lastUpdatedMillis()).isEqualTo(meta.lastUpdatedMillis());
    assertThat(result.lastColumnId()).isEqualTo(meta.lastColumnId());
    assertThat(result.schema().asStruct()).isEqualTo(meta.schema().asStruct());
    assertSameSchemaList(meta.schemas(), result.schemas());
    assertThat(result.currentSchemaId()).isEqualTo(meta.currentSchemaId());
    assertThat(result.defaultSpecId()).isEqualTo(meta.defaultSpecId());
    assertThat(result.specs()).isEqualTo(meta.specs());
    assertThat(result.properties()).isEqualTo(meta.properties());
    assertThat(result.currentSnapshot().snapshotId())
        .isEqualTo(meta.currentSnapshot().snapshotId());
    assertThat(Lists.transform(result.snapshots(), Snapshot::snapshotId))
        .isEqualTo(Lists.transform(meta.snapshots(), Snapshot::snapshotId));
    assertThat(result.snapshotLog()).isEqualTo(meta.snapshotLog());
  }
}
