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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestPartitionUtil {
  private static final int SOURCE_ID = 1;

  private static final Schema HISTORICAL_SCHEMA =
      new Schema(Types.NestedField.optional(SOURCE_ID, "p", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(HISTORICAL_SCHEMA).identity("p").build();

  @Test
  public void testHistoricalIdentityPartitionType() {
    FileScanTask task = taskWithUnknownPartitionType("a");

    Map<Integer, ?> constants =
        PartitionUtil.constantsMap(task, null, HISTORICAL_SCHEMA, (type, value) -> type.typeId());

    assertThat(constants.get(SOURCE_ID)).isEqualTo(Types.StringType.get().typeId());
  }

  @Test
  public void testExistingOverloadPreservesUnknownType() {
    FileScanTask task = taskWithUnknownPartitionType("a");

    Map<Integer, ?> constants =
        PartitionUtil.constantsMap(task, null, (type, value) -> type.typeId());

    assertThat(constants.get(SOURCE_ID)).isEqualTo(Types.UnknownType.get().typeId());
  }

  @Test
  public void testMissingHistoricalFieldPreservesUnknownType() {
    FileScanTask task = taskWithUnknownPartitionType("a");

    Map<Integer, ?> constants =
        PartitionUtil.constantsMap(task, null, new Schema(), (type, value) -> type.typeId());

    assertThat(constants.get(SOURCE_ID)).isEqualTo(Types.UnknownType.get().typeId());
  }

  @Test
  public void testKnownPartitionTypeIsPreserved() {
    FileScanTask task = task(SPEC, "a");

    Map<Integer, ?> constants =
        PartitionUtil.constantsMap(task, null, HISTORICAL_SCHEMA, (type, value) -> type.typeId());

    assertThat(constants.get(SOURCE_ID)).isEqualTo(Types.StringType.get().typeId());
  }

  @Test
  public void testNullHistoricalPartitionConstant() {
    FileScanTask task = taskWithUnknownPartitionType(null);

    Map<Integer, ?> constants =
        PartitionUtil.constantsMap(task, null, HISTORICAL_SCHEMA, (type, value) -> value);

    assertThat(constants).containsKey(SOURCE_ID);
    assertThat(constants.get(SOURCE_ID)).isNull();
  }

  private FileScanTask taskWithUnknownPartitionType(Object value) {
    PartitionSpec spec = mock(PartitionSpec.class);

    when(spec.fields()).thenReturn(SPEC.fields());
    when(spec.partitionType())
        .thenReturn(
            Types.StructType.of(
                List.of(
                    Types.NestedField.optional(
                        SPEC.fields().get(0).fieldId(), "p", Types.UnknownType.get()))));

    return task(spec, value);
  }

  private FileScanTask task(PartitionSpec spec, Object value) {
    FileScanTask task = mock(FileScanTask.class);
    DataFile file = mock(DataFile.class);
    StructLike partition = mock(StructLike.class);

    when(task.spec()).thenReturn(spec);
    when(task.file()).thenReturn(file);
    when(file.partition()).thenReturn(partition);
    when(file.location()).thenReturn("file.parquet");
    when(partition.get(0, Object.class)).thenReturn(value);

    return task;
  }
}
