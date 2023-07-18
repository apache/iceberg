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
package org.apache.iceberg.parquet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaVisitor;
import org.apache.iceberg.avro.UUIDConversion;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;

class ParquetAvro {

  private ParquetAvro() {}

  static Schema parquetAvroSchema(Schema avroSchema) {
    return AvroSchemaVisitor.visit(avroSchema, new ParquetDecimalSchemaConverter());
  }

  static class ParquetDecimal extends LogicalType {
    private static final String NAME = "parquet-decimal";

    private final int precision;
    private final int scale;

    ParquetDecimal(int precision, int scale) {
      super(NAME);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public String getName() {
      return NAME;
    }

    int precision() {
      return precision;
    }

    int scale() {
      return scale;
    }

    @Override
    public Schema addToSchema(Schema schema) {
      super.addToSchema(schema);
      schema.addProp("precision", String.valueOf(precision));
      schema.addProp("scale", String.valueOf(scale));
      return schema;
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      switch (schema.getType()) {
        case INT:
          Preconditions.checkArgument(
              precision <= 9, "Int cannot hold decimal precision: %s", precision);
          break;
        case LONG:
          Preconditions.checkArgument(
              precision <= 18, "Long cannot hold decimal precision: %s", precision);
          break;
        case FIXED:
          break;
        default:
          throw new IllegalArgumentException("Invalid base type for decimal: " + schema);
      }
      Preconditions.checkArgument(scale >= 0, "Scale %s cannot be negative", scale);
      Preconditions.checkArgument(
          scale <= precision, "Scale %s cannot be less than precision %s", scale, precision);
    }
  }

  static {
    LogicalTypes.register(
        ParquetDecimal.NAME,
        schema -> {
          int precision = Integer.parseInt(schema.getProp("precision"));
          int scale = Integer.parseInt(schema.getProp("scale"));
          return new ParquetDecimal(precision, scale);
        });
  }

  private static class IntDecimalConversion extends Conversion<BigDecimal> {
    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public String getLogicalTypeName() {
      return ParquetDecimal.NAME;
    }

    @Override
    public BigDecimal fromInt(Integer value, Schema schema, LogicalType type) {
      return BigDecimal.valueOf(value, ((ParquetDecimal) type).scale());
    }

    @Override
    public Integer toInt(BigDecimal value, Schema schema, LogicalType type) {
      return value.unscaledValue().intValue();
    }
  }

  private static class LongDecimalConversion extends Conversion<BigDecimal> {
    @Override
    public Class<BigDecimal> getConvertedType() {
      return BigDecimal.class;
    }

    @Override
    public String getLogicalTypeName() {
      return ParquetDecimal.NAME;
    }

    @Override
    public BigDecimal fromLong(Long value, Schema schema, LogicalType type) {
      return BigDecimal.valueOf(value, ((ParquetDecimal) type).scale());
    }

    @Override
    public Long toLong(BigDecimal value, Schema schema, LogicalType type) {
      return value.unscaledValue().longValue();
    }
  }

  private static class FixedDecimalConversion extends Conversions.DecimalConversion {
    private final WeakHashMap<Pair<Integer, Integer>, LogicalType> decimalsByScale;

    private FixedDecimalConversion() {
      this.decimalsByScale = new WeakHashMap<>();
    }

    @Override
    public String getLogicalTypeName() {
      return ParquetDecimal.NAME;
    }

    @Override
    public BigDecimal fromFixed(GenericFixed value, Schema schema, LogicalType type) {
      ParquetDecimal dec = (ParquetDecimal) type;
      return new BigDecimal(new BigInteger(value.bytes()), dec.scale());
    }

    @Override
    public GenericFixed toFixed(BigDecimal value, Schema schema, LogicalType type) {
      ParquetDecimal dec = (ParquetDecimal) type;
      Pair<Integer, Integer> key = new Pair<>(dec.precision(), dec.scale());
      return super.toFixed(
          value,
          schema,
          decimalsByScale.computeIfAbsent(
              key, k -> LogicalTypes.decimal(k.getFirst(), k.getSecond())));
    }
  }

  static final GenericData DEFAULT_MODEL =
      new SpecificData() {
        private final Conversion<?> fixedDecimalConversion = new FixedDecimalConversion();
        private final Conversion<?> intDecimalConversion = new IntDecimalConversion();
        private final Conversion<?> longDecimalConversion = new LongDecimalConversion();
        private final Conversion<?> uuidConversion = new UUIDConversion();

        {
          addLogicalTypeConversion(fixedDecimalConversion);
          addLogicalTypeConversion(uuidConversion);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Conversion<T> getConversionByClass(
            Class<T> datumClass, LogicalType logicalType) {
          if (logicalType == null) {
            return null;
          }

          if (logicalType instanceof ParquetDecimal) {
            ParquetDecimal decimal = (ParquetDecimal) logicalType;
            if (decimal.precision() <= 9) {
              return (Conversion<T>) intDecimalConversion;
            } else if (decimal.precision() <= 18) {
              return (Conversion<T>) longDecimalConversion;
            } else {
              return (Conversion<T>) fixedDecimalConversion;
            }
          } else if ("uuid".equals(logicalType.getName())) {
            return (Conversion<T>) uuidConversion;
          }
          return super.getConversionByClass(datumClass, logicalType);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Conversion<Object> getConversionFor(LogicalType logicalType) {
          if (logicalType == null) {
            return null;
          }

          if (logicalType instanceof LogicalTypes.Decimal) {
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            if (decimal.getPrecision() <= 9) {
              return (Conversion<Object>) intDecimalConversion;
            } else if (decimal.getPrecision() <= 18) {
              return (Conversion<Object>) longDecimalConversion;
            } else {
              return (Conversion<Object>) fixedDecimalConversion;
            }
          } else if ("uuid".equals(logicalType.getName())) {
            return (Conversion<Object>) uuidConversion;
          }
          return super.getConversionFor(logicalType);
        }
      };

  private static class ParquetDecimalSchemaConverter extends AvroSchemaVisitor<Schema> {
    @Override
    public Schema record(Schema record, List<String> names, List<Schema> types) {
      List<Schema.Field> fields = record.getFields();
      int length = fields.size();

      boolean hasChange = false;
      if (length != types.size()) {
        hasChange = true;
      }

      List<Schema.Field> newFields = Lists.newArrayListWithExpectedSize(length);
      for (int i = 0; i < length; i += 1) {
        Schema.Field field = fields.get(i);
        Schema type = types.get(i);

        newFields.add(copyField(field, type));

        if (!Objects.equal(field.schema(), type)) {
          hasChange = true;
        }
      }

      if (hasChange) {
        return copyRecord(record, newFields);
      }

      return record;
    }

    @Override
    public Schema union(Schema union, List<Schema> options) {
      if (!isIdentical(union.getTypes(), options)) {
        return Schema.createUnion(options);
      }
      return union;
    }

    @Override
    public Schema array(Schema array, Schema element) {
      if (!Objects.equal(array.getElementType(), element)) {
        return Schema.createArray(element);
      }
      return array;
    }

    @Override
    public Schema map(Schema map, Schema value) {
      if (!Objects.equal(map.getValueType(), value)) {
        return Schema.createMap(value);
      }
      return map;
    }

    @Override
    public Schema primitive(Schema primitive) {
      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null && logicalType instanceof LogicalTypes.Decimal) {
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
        if (decimal.getPrecision() <= 9) {
          return new ParquetDecimal(decimal.getPrecision(), decimal.getScale())
              .addToSchema(Schema.create(Schema.Type.INT));

        } else if (decimal.getPrecision() <= 18) {
          return new ParquetDecimal(decimal.getPrecision(), decimal.getScale())
              .addToSchema(Schema.create(Schema.Type.LONG));

        } else {
          return new ParquetDecimal(decimal.getPrecision(), decimal.getScale())
              .addToSchema(
                  Schema.createFixed(
                      primitive.getName(),
                      null,
                      null,
                      TypeUtil.decimalRequiredBytes(decimal.getPrecision())));
        }
      }

      return primitive;
    }

    private boolean isIdentical(List<Schema> types, List<Schema> replacements) {
      if (types.size() != replacements.size()) {
        return false;
      }

      int length = types.size();
      for (int i = 0; i < length; i += 1) {
        if (!Objects.equal(types.get(i), replacements.get(i))) {
          return false;
        }
      }

      return true;
    }

    private static Schema copyRecord(Schema record, List<Schema.Field> newFields) {
      Schema copy =
          Schema.createRecord(
              record.getName(),
              record.getDoc(),
              record.getNamespace(),
              record.isError(),
              newFields);

      for (Map.Entry<String, Object> prop : record.getObjectProps().entrySet()) {
        copy.addProp(prop.getKey(), prop.getValue());
      }

      return copy;
    }

    private static Schema.Field copyField(Schema.Field field, Schema newSchema) {
      Schema.Field copy =
          new Schema.Field(field.name(), newSchema, field.doc(), field.defaultVal(), field.order());

      for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
        copy.addProp(prop.getKey(), prop.getValue());
      }

      return copy;
    }
  }

  private static class Pair<K, V> {
    private final K first;
    private final V second;

    Pair(final K first, final V second) {
      this.first = first;
      this.second = second;
    }

    public static <K, V> Pair<K, V> of(K first, V second) {
      return new Pair<>(first, second);
    }

    public K getFirst() {
      return first;
    }

    public V getSecond() {
      return second;
    }
  }
}
