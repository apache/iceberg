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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequestParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestCatalogHandlers {
  private static final int LAST_ADDED = -1;

  @TempDir private Path temp;

  @AfterEach
  public void clearTables() {
    TestTables.clearTables();
  }

  @Test
  public void commitAddSchemaWithoutDeprecatedLastColumnIdPreservesTableLastColumnId() {
    Schema baseSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "dropme", Types.StringType.get()));
    TestTables.TestTableOperations ops = new TestTables.TestTableOperations("test", temp.toFile());
    TestTables.create(
        temp.toFile(),
        "test",
        baseSchema,
        PartitionSpec.unpartitioned(),
        SortOrder.unsorted(),
        2,
        ops);
    TableMetadata base = ops.current();
    assertThat(base.lastColumnId()).isEqualTo(2);

    Schema schemaAfterDroppingHighestId =
        new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    List<MetadataUpdate> updates =
        ImmutableList.of(
            new MetadataUpdate.AddSchema(schemaAfterDroppingHighestId),
            new MetadataUpdate.SetCurrentSchema(LAST_ADDED));
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = new UpdateTableRequest(requirements, updates);

    String json = UpdateTableRequestParser.toJson(request, true);
    assertThat(json).doesNotContain("last-column-id");

    TableMetadata updated = CatalogHandlers.commit(ops, UpdateTableRequestParser.fromJson(json));

    assertThat(updated.schema().highestFieldId()).isEqualTo(1);
    assertThat(updated.lastColumnId()).isEqualTo(2);
  }
}
