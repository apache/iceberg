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
package org.apache.iceberg.avro;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.RandomVariants;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;

public class RandomAvroData {

  private RandomAvroData() {}

  public static List<Record> generate(Schema schema, int numRecords, long seed) {
    RandomDataGenerator generator = new RandomDataGenerator(schema, seed);
    List<Record> records = Lists.newArrayListWithExpectedSize(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      records.add((Record) TypeUtil.visit(schema, generator));
    }

    return records;
  }

  private static class RandomDataGenerator extends TypeUtil.CustomOrderSchemaVisitor<Object> {
    private final Map<Type, org.apache.avro.Schema> typeToSchema;
    private final Random random;

    private RandomDataGenerator(Schema schema, long seed) {
      this.typeToSchema = AvroSchemaUtil.convertTypes(schema.asStruct(), "test");
      this.random = new Random(seed);
    }

    @Override
    public Record schema(Schema schema, Supplier<Object> structResult) {
      return (Record) structResult.get();
    }

    @Override
    public Record struct(Types.StructType struct, Iterable<Object> fieldResults) {
      Record rec = new Record(typeToSchema.get(struct));

      List<Object> values = Lists.newArrayList(fieldResults);
      for (int i = 0; i < values.size(); i += 1) {
        rec.put(i, values.get(i));
      }

      return rec;
    }

    @Override
    public Object field(Types.NestedField field, Supplier<Object> fieldResult) {
      // return null 5% of the time when the value is optional
      if (field.isOptional() && random.nextInt(20) == 1) {
        return null;
      }
      return fieldResult.get();
    }

    @Override
    public Object list(Types.ListType list, Supplier<Object> elementResult) {
      return RandomUtil.generateList(random, list, elementResult);
    }

    @Override
    public Object map(Types.MapType map, Supplier<Object> keyResult, Supplier<Object> valueResult) {
      return RandomUtil.generateMap(random, map, keyResult, valueResult);
    }

    @Override
    public Object variant(Types.VariantType variant) {
      return RandomVariants.randomVariant(random);
    }

    @Override
    public Object primitive(Type.PrimitiveType primitive) {
      Object result = RandomUtil.generatePrimitive(primitive, random);
      // For the primitives that Avro needs a different type than Spark, fix
      // them here.
      switch (primitive.typeId()) {
        case STRING:
          return new Utf8((String) result);
        case FIXED:
          return new GenericData.Fixed(typeToSchema.get(primitive), (byte[]) result);
        case BINARY:
          return ByteBuffer.wrap((byte[]) result);
        case UUID:
          return UUID.nameUUIDFromBytes((byte[]) result);
        default:
          return result;
      }
    }
  }
}
