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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestGenericRecord {

  @Test
  public void testGetNullValue() {
    Types.LongType type = Types.LongType.get();
    Schema schema = new Schema(optional(1, "id", type));
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, null);

    assertThat(record.get(0, type.typeId().javaClass())).isNull();
  }

  @Test
  public void testGetNotNullValue() {
    Types.LongType type = Types.LongType.get();
    Schema schema = new Schema(optional(1, "id", type));
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, 10L);

    assertThat(record.get(0, type.typeId().javaClass())).isEqualTo(10L);
  }

  @Test
  public void testGetIncorrectClassInstance() {
    Schema schema = new Schema(optional(1, "id", Types.LongType.get()));
    GenericRecord record = GenericRecord.create(schema);
    record.set(0, 10L);

    assertThatThrownBy(() -> record.get(0, CharSequence.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Not an instance of java.lang.CharSequence: 10");
  }
}
