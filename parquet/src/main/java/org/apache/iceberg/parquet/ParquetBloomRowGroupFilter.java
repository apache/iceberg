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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.FindsResidualVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.BloomFilterReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetBloomRowGroupFilter {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetBloomRowGroupFilter.class);

  private final Schema schema;
  private final Expression expr;
  private final boolean caseSensitive;

  public ParquetBloomRowGroupFilter(Schema schema, Expression expr) {
    this(schema, expr, true);
  }

  public ParquetBloomRowGroupFilter(Schema schema, Expression expr, boolean caseSensitive) {
    this.schema = schema;
    StructType struct = schema.asStruct();
    if (Binder.isBound(expr)) {
      this.expr = Expressions.rewriteNot(expr);
    } else {
      this.expr = Binder.bind(struct, Expressions.rewriteNot(expr), caseSensitive);
    }

    this.caseSensitive = caseSensitive;
  }

  /**
   * Tests whether the bloom for a row group may contain records that match the expression.
   *
   * @param fileSchema schema for the Parquet file
   * @param rowGroup metadata for a row group
   * @param bloomReader a bloom filter reader
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean shouldRead(
      MessageType fileSchema, BlockMetaData rowGroup, BloomFilterReader bloomReader) {
    return residualFor(fileSchema, rowGroup, bloomReader) != Expressions.alwaysFalse();
  }

  public Expression residualFor(
      MessageType fileSchema, BlockMetaData rowGroup, BloomFilterReader bloomReader) {
    return new BloomEvalVisitor().eval(fileSchema, rowGroup, bloomReader);
  }

  private class BloomEvalVisitor extends FindsResidualVisitor {
    private BloomFilterReader bloomReader;
    private Set<Integer> fieldsWithBloomFilter = null;
    private Map<Integer, ColumnChunkMetaData> columnMetaMap = null;
    private Map<Integer, BloomFilter> bloomCache = null;
    private Map<Integer, PrimitiveType> parquetPrimitiveTypes = null;
    private Map<Integer, Type> types = null;

    private Expression eval(
        MessageType fileSchema, BlockMetaData rowGroup, BloomFilterReader bloomFilterReader) {
      this.bloomReader = bloomFilterReader;
      this.fieldsWithBloomFilter = Sets.newHashSet();
      this.columnMetaMap = Maps.newHashMap();
      this.bloomCache = Maps.newHashMap();
      this.parquetPrimitiveTypes = Maps.newHashMap();
      this.types = Maps.newHashMap();

      for (ColumnChunkMetaData meta : rowGroup.getColumns()) {
        PrimitiveType colType = fileSchema.getType(meta.getPath().toArray()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          Type icebergType = schema.findType(id);
          if (!ParquetUtil.hasNoBloomFilterPages(meta)) {
            fieldsWithBloomFilter.add(id);
          }
          columnMetaMap.put(id, meta);
          parquetPrimitiveTypes.put(id, colType);
          types.put(id, icebergType);
        }
      }

      Set<Integer> filterRefs =
          Binder.boundReferences(schema.asStruct(), ImmutableList.of(expr), caseSensitive);
      if (!filterRefs.isEmpty()) {
        Set<Integer> overlappedBloomFilters = Sets.intersection(fieldsWithBloomFilter, filterRefs);
        if (overlappedBloomFilters.isEmpty()) {
          return expr;
        } else {
          LOG.debug("Using Bloom filters for columns with IDs: {}", overlappedBloomFilters);
        }
      }

      return ExpressionVisitors.visit(expr, this);
    }

    @Override
    public Expression alwaysTrue() {
      return ROWS_ALL_MATCH; // all rows match
    }

    @Override
    public Expression alwaysFalse() {
      return ROWS_CANNOT_MATCH; // all rows fail
    }

    @Override
    public Expression not(Expression result) {
      // not() should be rewritten by RewriteNot
      // bloom filter is based on hash and cannot eliminate based on not
      throw new UnsupportedOperationException("This path shouldn't be reached.");
    }

    @Override
    public Expression and(Expression leftResult, Expression rightResult) {
      return Expressions.and(leftResult, rightResult);
    }

    @Override
    public Expression or(Expression leftResult, Expression rightResult) {
      return Expressions.or(leftResult, rightResult);
    }

    @Override
    public <T> Expression isNull(BoundReference<T> ref) {
      // bloom filter only contain non-nulls and cannot eliminate based on isNull or NotNull
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression notNull(BoundReference<T> ref) {
      // bloom filter only contain non-nulls and cannot eliminate based on isNull or NotNull
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression isNaN(BoundReference<T> ref) {
      // bloom filter is based on hash and cannot eliminate based on isNaN or notNaN
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression notNaN(BoundReference<T> ref) {
      // bloom filter is based on hash and cannot eliminate based on isNaN or notNaN
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression lt(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression ltEq(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression gt(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression gtEq(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on lt or ltEq or gt or gtEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression eq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();
      if (!fieldsWithBloomFilter.contains(id)) { // no bloom filter
        return ROWS_MIGHT_MATCH;
      }

      BloomFilter bloom = loadBloomFilter(id);
      Type type = types.get(id);
      T value = lit.value();
      return shouldRead(parquetPrimitiveTypes.get(id), value, bloom, type)
          ? ROWS_MIGHT_MATCH
          : ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Expression notEq(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on notEq
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression in(BoundReference<T> ref, Set<T> literalSet) {
      int id = ref.fieldId();
      if (!fieldsWithBloomFilter.contains(id)) { // no bloom filter
        return ROWS_MIGHT_MATCH;
      }
      BloomFilter bloom = loadBloomFilter(id);
      Type type = types.get(id);
      for (T e : literalSet) {
        if (shouldRead(parquetPrimitiveTypes.get(id), e, bloom, type)) {
          return ROWS_MIGHT_MATCH;
        }
      }
      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Expression notIn(BoundReference<T> ref, Set<T> literalSet) {
      // bloom filter is based on hash and cannot eliminate based on notIn
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression startsWith(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on startsWith
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Expression notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      // bloom filter is based on hash and cannot eliminate based on startsWith
      return ROWS_MIGHT_MATCH;
    }

    private BloomFilter loadBloomFilter(int id) {
      if (bloomCache.containsKey(id)) {
        return bloomCache.get(id);
      } else {
        ColumnChunkMetaData columnChunkMetaData = columnMetaMap.get(id);
        BloomFilter bloomFilter = bloomReader.readBloomFilter(columnChunkMetaData);
        if (bloomFilter == null) {
          throw new IllegalStateException("Failed to read required bloom filter for id: " + id);
        } else {
          bloomCache.put(id, bloomFilter);
        }

        return bloomFilter;
      }
    }

    private <T> boolean shouldRead(
        PrimitiveType primitiveType, T value, BloomFilter bloom, Type type) {
      long hashValue = 0;
      switch (primitiveType.getPrimitiveTypeName()) {
        case INT32:
          switch (type.typeId()) {
            case DECIMAL:
              BigDecimal decimalValue = (BigDecimal) value;
              hashValue = bloom.hash(decimalValue.unscaledValue().intValue());
              return bloom.findHash(hashValue);
            case INTEGER:
            case DATE:
              hashValue = bloom.hash(((Number) value).intValue());
              return bloom.findHash(hashValue);
            default:
              return true;  /* rows might match */
          }
        case INT64:
          switch (type.typeId()) {
            case DECIMAL:
              BigDecimal decimalValue = (BigDecimal) value;
              hashValue = bloom.hash(decimalValue.unscaledValue().longValue());
              return bloom.findHash(hashValue);
            case LONG:
            case TIME:
            case TIMESTAMP:
              hashValue = bloom.hash(((Number) value).longValue());
              return bloom.findHash(hashValue);
            default:
              return true;  /* rows might match */
          }
        case FLOAT:
          hashValue = bloom.hash(((Number) value).floatValue());
          return bloom.findHash(hashValue);
        case DOUBLE:
          hashValue = bloom.hash(((Number) value).doubleValue());
          return bloom.findHash(hashValue);
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          switch (type.typeId()) {
            case STRING:
              hashValue = bloom.hash(Binary.fromCharSequence((CharSequence) value));
              return bloom.findHash(hashValue);
            case BINARY:
            case FIXED:
              hashValue = bloom.hash(Binary.fromConstantByteBuffer((ByteBuffer) value));
              return bloom.findHash(hashValue);
            case DECIMAL:
              DecimalLogicalTypeAnnotation metadata =
                  (DecimalLogicalTypeAnnotation) primitiveType.getLogicalTypeAnnotation();
              int scale = metadata.getScale();
              int precision = metadata.getPrecision();
              byte[] requiredBytes = new byte[TypeUtil.decimalRequiredBytes(precision)];
              byte[] binary =
                  DecimalUtil.toReusedFixLengthBytes(
                      precision, scale, (BigDecimal) value, requiredBytes);
              hashValue = bloom.hash(Binary.fromConstantByteArray(binary));
              return bloom.findHash(hashValue);
            case UUID:
              hashValue = bloom.hash(Binary.fromConstantByteArray(UUIDUtil.convert((UUID) value)));
              return bloom.findHash(hashValue);
            default:
              return true;  /* rows might match */
          }
        default:
          return true;  /* rows might match */
      }
    }

    @Override
    public <T> Expression handleNonReference(Bound<T> term) {
      return ROWS_MIGHT_MATCH;
    }
  }
}
