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
package org.apache.iceberg.functions;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.Test;

class TestFunctionSerialization {
  private static final Type[] TYPES =
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
        Types.TimestampType.withZone(),
        Types.TimestampNanoType.withoutZone(),
        Types.BinaryType.get(),
        Types.FixedType.ofLength(4),
        Types.DecimalType.of(9, 4),
        Types.UUIDType.get(),
        Types.StructType.of(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "payload", Types.BinaryType.get())),
      };

  private static final IcebergFunction<?, ?>[] FUNCTIONS =
      new IcebergFunction<?, ?>[] {
        IcebergFunctions.maskAlphanum(),
        IcebergFunctions.maskToFixedValue(),
        IcebergFunctions.replaceWithNull(),
        IcebergFunctions.showFirst4(),
        IcebergFunctions.showLast4(),
        IcebergFunctions.truncateToYear(),
        IcebergFunctions.truncateToMonth(),
        IcebergFunctions.sha256Global(),
        IcebergFunctions.sha256QueryLocal()
      };

  @Test
  void functionsDeserializeToTheSameInstance() throws Exception {
    for (IcebergFunction<?, ?> function : FUNCTIONS) {
      assertThat(TestHelpers.roundTripSerialize(function)).isSameAs(function);
    }
  }

  @Test
  void unknownFunctionDeserializesToAnEqualFunction() throws Exception {
    IcebergFunction<?, ?> unknown = IcebergFunctions.fromString("future-mask-v2");
    assertThat(TestHelpers.roundTripSerialize(unknown)).isEqualTo(unknown);
  }

  @Test
  void boundFunctionsAreSerializable() throws Exception {
    byte[] salt = new byte[16];
    for (Type type : TYPES) {
      for (IcebergFunction<?, ?> function : FUNCTIONS) {
        if (function.canBind(type)) {
          SerializableFunction<?, ?> func =
              function instanceof SaltedFunction
                  ? ((SaltedFunction<?, ?>) function).bind(type, salt)
                  : function.bind(type);
          assertThat(func).isInstanceOf(TestHelpers.roundTripSerialize(func).getClass());
        }
      }
    }
  }
}
