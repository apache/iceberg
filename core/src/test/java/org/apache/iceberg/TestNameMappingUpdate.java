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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestNameMappingUpdate {

  private final UpdateProperties updateProperties = mock(UpdateProperties.class);
  private static final Schema SCHEMA = mkSchema();

  private final UpdateNameMapping updateNameMapping =
      new NameMappingUpdate(
          SCHEMA,
          MappingUtil.create(SCHEMA),
          updateProperties);

  @Test
  public void testAddAliasesForField() {
    NameMapping mapping =
        updateNameMapping
            .addAliases("metadata", "row_id", ImmutableList.of("rowId"))
            .addAliases("metadata", "ingestion_timestamp_ms", ImmutableList.of("event_utc_ms"))
            .apply();

    int rowIdFieldId = SCHEMA.findField("metadata" + "." + "row_id").fieldId();
    int ingestionTimestampFieldId = SCHEMA.findField("metadata" + "." + "ingestion_timestamp_ms")
        .fieldId();

    assertEquals(mapping.find(rowIdFieldId).names(), ImmutableSet.of("rowId", "row_id"));
    assertEquals(mapping.find(ingestionTimestampFieldId).names(),
        ImmutableSet.of("ingestion_timestamp_ms", "event_utc_ms"));
  }

  @Test
  public void testAddingDuplicateAliasesForField() {
    NameMapping mapping =
        updateNameMapping
            .addAliases("metadata", "row_id", ImmutableList.of("rowId"))
            .addAliases("metadata", "row_id", ImmutableList.of("rowId"))
            .apply();

    int rowIdFieldId = SCHEMA.findField("metadata" + "." + "row_id").fieldId();

    assertEquals(mapping.find(rowIdFieldId).names(), ImmutableSet.of("rowId", "row_id"));
  }

  @Test
  public void testAddingAliasForStructField() {
    NameMapping mapping =
        updateNameMapping
            .addAliases("metadata", ImmutableList.of("meta"))
            .apply();

    int metadataFieldId = SCHEMA.findField("metadata").fieldId();

    assertEquals(mapping.find(metadataFieldId).names(), ImmutableSet.of("meta", "metadata"));
  }

  static Schema mkSchema() {
    final Types.StructType struct = Types.StructType.of(
        Types.NestedField.optional(1, "properties",
            Types.MapType.ofOptional(7, 8,
                Types.StringType.get(), Types.StringType.get())),
        Types.NestedField.optional(3, "dateint", Types.IntegerType.get()),
        Types.NestedField.optional(6, "metadata",
            Types.StructType.of(
                // ingestion metadata - represents metadata regarding the producer of this event
                Types.NestedField.optional(11, "ingestion_timestamp_ms", Types.LongType.get()),
                Types.NestedField.optional(12, "hostname", Types.StringType.get()),
                Types.NestedField.optional(15, "row_id", Types.StringType.get())
            ))
    );
    return new Schema(struct.fields());
  }
}
