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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestStructTransform {
  @Test
  public void testInvalidSourceField() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "int", Types.IntegerType.get()));
    List<StructTransform.FieldTransform> fieldTransforms =
        Arrays.asList(new StructTransform.FieldTransform(3, Transforms.identity()));
    assertThatThrownBy(() -> new StructTransform(schema, fieldTransforms))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find source field: 3");
  }

  @Test
  public void testResultSchema() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.required(2, "ratio", Types.DoubleType.get()),
            Types.NestedField.optional(
                3,
                "user",
                Types.StructType.of(
                    Types.NestedField.required(11, "name", Types.StringType.get()),
                    Types.NestedField.required(12, "ts", Types.TimestampType.withoutZone()),
                    Types.NestedField.optional(13, "device_id", Types.UUIDType.get()),
                    Types.NestedField.optional(
                        14,
                        "location",
                        Types.StructType.of(
                            Types.NestedField.required(101, "lat", Types.FloatType.get()),
                            Types.NestedField.required(102, "long", Types.FloatType.get()),
                            Types.NestedField.required(103, "blob", Types.BinaryType.get()))))));

    List<StructTransform.FieldTransform> fieldTransforms =
        Arrays.asList(
            new StructTransform.FieldTransform(2, Transforms.identity()),
            new StructTransform.FieldTransform(12, Transforms.hour()),
            new StructTransform.FieldTransform(13, Transforms.bucket(16)),
            new StructTransform.FieldTransform(103, Transforms.truncate(16)));

    StructTransform structTransform = new StructTransform(schema, fieldTransforms);
    assertThat(structTransform.resultSchema().asStruct())
        .isEqualTo(
            Types.StructType.of(
                Types.NestedField.required(2, "ratio", Types.DoubleType.get()),
                Types.NestedField.required(12, "ts", Types.IntegerType.get()),
                Types.NestedField.optional(13, "device_id", Types.IntegerType.get()),
                Types.NestedField.required(103, "blob", Types.BinaryType.get())));
  }
}
