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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestCombinedRecord {
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "id2", Types.IntegerType.get()),
          Types.NestedField.optional(4, "data2", Types.StringType.get()),
          Types.NestedField.optional(5, "id3", Types.IntegerType.get()),
          Types.NestedField.optional(6, "data3", Types.StringType.get()));

  @Test
  void testCombinedRecord() {
    GenericRecord idRecord = GenericRecord.create(SCHEMA.select("id", "id2", "id3"));
    idRecord.set(0, 1);
    idRecord.set(1, 2);
    idRecord.set(2, 3);
    GenericRecord dataRecord = GenericRecord.create(SCHEMA.select("data", "data2", "data3"));
    dataRecord.set(0, "one");
    dataRecord.set(1, "two");
    dataRecord.set(2, "three");
    CombinedRecord combinedRecord =
        CombinedRecord.create(SCHEMA, new Integer[] {1, 3, 5}, new Integer[] {2, 4, 6});
    combinedRecord.setFamily(0, idRecord);
    combinedRecord.setFamily(1, dataRecord);

    GenericRecord expected = GenericRecord.create(SCHEMA);
    expected.set(0, 1);
    expected.set(2, 2);
    expected.set(4, 3);
    expected.set(1, "one");
    expected.set(3, "two");
    expected.set(5, "three");

    assertThat(combinedRecord.copy()).isEqualTo(expected);
  }
}
