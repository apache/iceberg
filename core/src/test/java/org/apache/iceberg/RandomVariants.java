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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

public class RandomVariants {
  private RandomVariants() {}

  public static Variant randomVariant(Random random) {
    PhysicalType type = randomType(random);
    VariantMetadata metadata;
    if (type == PhysicalType.OBJECT || type == PhysicalType.ARRAY) {
      int numFields = random.nextInt(25) + 5; // at least 5 keys
      List<String> randomNames = Lists.newArrayList();
      for (int i = 0; i < numFields; i += 1) {
        randomNames.add(RandomUtil.generateString(10, random));
      }
      metadata =
          VariantMetadata.from(VariantTestUtil.createMetadata(randomNames, random.nextBoolean()));
    } else {
      metadata = VariantMetadata.empty();
    }

    return Variant.of(metadata, randomVariant(random, metadata, randomType(random)));
  }

  private static PhysicalType randomType(Random random) {
    PhysicalType[] types = PhysicalType.values();
    return types[random.nextInt(types.length)];
  }

  private static VariantValue randomVariant(
      Random random, VariantMetadata metadata, PhysicalType type) {
    switch (type) {
      case NULL:
        return Variants.ofNull();
      case BOOLEAN_TRUE:
        return Variants.of(true);
      case BOOLEAN_FALSE:
        return Variants.of(false);
      case INT8:
        return Variants.of(
            type,
            ((Integer) RandomUtil.generatePrimitive(Types.IntegerType.get(), random)).byteValue());
      case INT16:
        return Variants.of(
            type,
            ((Integer) RandomUtil.generatePrimitive(Types.IntegerType.get(), random)).shortValue());
      case INT32:
        return Variants.of(type, RandomUtil.generatePrimitive(Types.IntegerType.get(), random));
      case INT64:
        return Variants.of(type, RandomUtil.generatePrimitive(Types.LongType.get(), random));
      case DOUBLE:
        return Variants.of(type, RandomUtil.generatePrimitive(Types.DoubleType.get(), random));
      case DECIMAL4:
        return Variants.of(
            type, RandomUtil.generatePrimitive(Types.DecimalType.of(9, random.nextInt(9)), random));
      case DECIMAL8:
        return Variants.of(
            type,
            RandomUtil.generatePrimitive(Types.DecimalType.of(18, random.nextInt(18)), random));
      case DECIMAL16:
        return Variants.of(
            type,
            RandomUtil.generatePrimitive(Types.DecimalType.of(38, random.nextInt(38)), random));
      case DATE:
        return Variants.of(type, RandomUtil.generatePrimitive(Types.DateType.get(), random));
      case TIMESTAMPTZ:
        return Variants.of(
            type, RandomUtil.generatePrimitive(Types.TimestampType.withZone(), random));
      case TIMESTAMPNTZ:
        return Variants.of(
            type, RandomUtil.generatePrimitive(Types.TimestampType.withoutZone(), random));
      case FLOAT:
        return Variants.of(type, RandomUtil.generatePrimitive(Types.FloatType.get(), random));
      case BINARY:
        byte[] randomBytes = (byte[]) RandomUtil.generatePrimitive(Types.BinaryType.get(), random);
        return Variants.of(type, ByteBuffer.wrap(randomBytes));
      case STRING:
        return Variants.of(type, RandomUtil.generatePrimitive(Types.StringType.get(), random));
      case TIME:
        return Variants.of(type, RandomUtil.generatePrimitive(Types.TimeType.get(), random));
      case TIMESTAMPTZ_NANOS:
        return Variants.of(
            type, RandomUtil.generatePrimitive(Types.TimestampNanoType.withZone(), random));
      case TIMESTAMPNTZ_NANOS:
        return Variants.of(
            type, RandomUtil.generatePrimitive(Types.TimestampNanoType.withoutZone(), random));
      case UUID:
        byte[] uuidBytes = (byte[]) RandomUtil.generatePrimitive(Types.UUIDType.get(), random);
        return Variants.of(type, UUIDUtil.convert(uuidBytes));
      case ARRAY:
        ValueArray arr = Variants.array();
        int numElements = random.nextInt(10);
        for (int i = 0; i < numElements; i += 1) {
          arr.add(randomVariant(random, metadata, randomType(random)));
        }

        return arr;
      case OBJECT:
        ShreddedObject object = Variants.object(metadata);
        if (metadata.dictionarySize() > 0) {
          int numFields = random.nextInt(10);
          for (int i = 0; i < numFields; i += 1) {
            String field = metadata.get(random.nextInt(metadata.dictionarySize()));
            object.put(field, randomVariant(random, metadata, randomType(random)));
          }
        }

        return object;
    }

    throw new UnsupportedOperationException("Unsupported variant type: " + type);
  }
}
