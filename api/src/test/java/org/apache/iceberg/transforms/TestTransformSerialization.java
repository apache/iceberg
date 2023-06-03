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
package org.apache.iceberg.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.Test;

public class TestTransformSerialization {
  @Test
  public void testFunctionSerialization() throws Exception {
    Type[] types =
        new Type[] {
          Types.BooleanType.get(),
          Types.IntegerType.get(),
          Types.LongType.get(),
          Types.FloatType.get(),
          Types.DoubleType.get(),
          Types.StringType.get(),
          Types.DateType.get(),
          Types.TimeType.get(),
          Types.TimestampType.withoutZone(),
          Types.TimestampType.withoutZone(),
          Types.BinaryType.get(),
          Types.FixedType.ofLength(4),
          Types.DecimalType.of(9, 4),
          Types.UUIDType.get(),
        };

    Transform<?, ?>[] transforms =
        new Transform<?, ?>[] {
          Transforms.identity(),
          Transforms.bucket(1024),
          Transforms.year(),
          Transforms.month(),
          Transforms.day(),
          Transforms.hour(),
          Transforms.truncate(16)
        };

    for (Type type : types) {
      for (Transform<?, ?> transform : transforms) {
        assertThat(TestHelpers.roundTripSerialize(transform)).isEqualTo(transform);

        if (transform.canTransform(type)) {
          SerializableFunction<?, ?> func = transform.bind(type);
          assertThat(func).isInstanceOf(TestHelpers.roundTripSerialize(func).getClass());
        }
      }
    }
  }
}
