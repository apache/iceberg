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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantTestUtil;

public class DataTestHelpers {
  private DataTestHelpers() {}

  public static void assertEquals(Types.StructType struct, Record expected, Record actual) {
    assertEquals(struct, expected, actual, null, -1);
  }

  public static void assertEquals(
      Types.StructType struct,
      Record expected,
      Record actual,
      Map<Integer, Object> idToConstant,
      int pos) {
    Types.StructType expectedType = expected.struct();
    for (Types.NestedField field : struct.fields()) {
      Types.NestedField expectedField = expectedType.field(field.fieldId());
      Object expectedValue;
      if (expectedField != null) {
        int id = expectedField.fieldId();
        if (id == MetadataColumns.ROW_ID.fieldId()) {
          expectedValue = expected.getField(expectedField.name());
          if (expectedValue == null && idToConstant != null) {
            expectedValue = (Long) idToConstant.get(id) + pos;
          }

        } else if (id == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()) {
          expectedValue = expected.getField(expectedField.name());
          if (expectedValue == null && idToConstant != null) {
            expectedValue = idToConstant.get(id);
          }

        } else {
          expectedValue = expected.getField(expectedField.name());
        }

        assertEquals(field.type(), expectedValue, actual.getField(field.name()));

      } else {
        assertEquals(
            field.type(),
            GenericDataUtil.internalToGeneric(field.type(), field.initialDefault()),
            actual.getField(field.name()));
      }
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

    assertThat(actual).as("Map size should match").hasSameSizeAs(expected);

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
      case UNKNOWN:
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_NANO:
      case UUID:
      case BINARY:
      case DECIMAL:
      case GEOMETRY:
      case GEOGRAPHY:
        assertThat(actual)
            .as("Primitive value should be equal to expected for type " + type)
            .isEqualTo(expected);
        break;
      case VARIANT:
        assertThat(expected).as("Expected should be a Variant").isInstanceOf(Variant.class);
        assertThat(actual).as("Actual should be a Variant").isInstanceOf(Variant.class);
        VariantTestUtil.assertEqual(((Variant) expected).metadata(), ((Variant) actual).metadata());
        VariantTestUtil.assertEqual(((Variant) expected).value(), ((Variant) actual).value());
        break;
      case FIXED:
        assertThat(expected).as("Expected should be a byte[]").isInstanceOf(byte[].class);
        assertThat(actual).as("Actual should be a byte[]").isInstanceOf(byte[].class);
        assertThat(actual).as("Array contents should be equal").isEqualTo(expected);
        break;
      case STRUCT:
        assertThat(expected).as("Expected should be a Record").isInstanceOf(Record.class);
        assertThat(actual).as("Actual should be a Record").isInstanceOf(Record.class);
        assertEquals(type.asStructType(), (Record) expected, (Record) actual);
        break;
      case LIST:
        assertThat(expected).as("Expected should be a List").isInstanceOf(List.class);
        assertThat(actual).as("Actual should be a List").isInstanceOf(List.class);
        assertEquals(type.asListType(), (List) expected, (List) actual);
        break;
      case MAP:
        assertThat(expected).as("Expected should be a Map").isInstanceOf(Map.class);
        assertThat(actual).as("Actual should be a Map").isInstanceOf(Map.class);
        assertEquals(type.asMapType(), (Map<?, ?>) expected, (Map<?, ?>) actual);
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }
}
