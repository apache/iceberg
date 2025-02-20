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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantTestUtil;

public class InternalTestHelpers {

  private InternalTestHelpers() {}

  public static void assertEquals(Types.StructType struct, StructLike expected, StructLike actual) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = expected.get(i, fieldType.typeId().javaClass());
      Object actualValue = actual.get(i, fieldType.typeId().javaClass());

      assertEquals(fieldType, expectedValue, actualValue);
    }
  }

  public static void assertEquals(Types.ListType list, List<?> expected, List<?> actual) {
    Type elementType = list.elementType();

    assertThat(actual).as("List size should match").hasSameSizeAs(expected);

    for (int i = 0; i < expected.size(); i += 1) {
      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i);

      assertEquals(elementType, expectedValue, actualValue);
    }
  }

  public static void assertEquals(Types.MapType map, Map<?, ?> expected, Map<?, ?> actual) {
    Type valueType = map.valueType();

    assertThat(actual).as("Map keys should match").hasSameSizeAs(expected);

    for (Object expectedKey : expected.keySet()) {
      Object expectedValue = expected.get(expectedKey);
      Object actualValue = actual.get(expectedKey);

      assertEquals(valueType, expectedValue, actualValue);
    }
  }

  private static void assertEquals(Type type, Object expected, Object actual) {
    if (expected == null && actual == null) {
      return;
    }

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case UUID:
      case FIXED:
      case BINARY:
      case DECIMAL:
        assertThat(actual).as("Primitive value should be equal to expected").isEqualTo(expected);
        break;
      case STRUCT:
        assertThat(expected).as("Expected should be a StructLike").isInstanceOf(StructLike.class);
        assertThat(actual).as("Actual should be a StructLike").isInstanceOf(StructLike.class);
        assertEquals(type.asStructType(), (StructLike) expected, (StructLike) actual);
        break;
      case LIST:
        assertThat(expected).as("Expected should be a List").isInstanceOf(List.class);
        assertThat(actual).as("Actual should be a List").isInstanceOf(List.class);
        assertEquals(type.asListType(), (List<?>) expected, (List<?>) actual);
        break;
      case MAP:
        assertThat(expected).as("Expected should be a Map").isInstanceOf(Map.class);
        assertThat(actual).as("Actual should be a Map").isInstanceOf(Map.class);
        assertEquals(type.asMapType(), (Map<?, ?>) expected, (Map<?, ?>) actual);
        break;
      case VARIANT:
        assertThat(expected).as("Expected should be a Variant").isInstanceOf(Variant.class);
        assertThat(actual).as("Actual should be a Variant").isInstanceOf(Variant.class);
        VariantTestUtil.assertEqual(((Variant) expected).metadata(), ((Variant) actual).metadata());
        VariantTestUtil.assertEqual(((Variant) expected).value(), ((Variant) actual).value());
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }
}
