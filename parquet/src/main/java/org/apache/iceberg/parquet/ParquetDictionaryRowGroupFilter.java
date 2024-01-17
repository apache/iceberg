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

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.NaNUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetDictionaryRowGroupFilter {
  private final Schema schema;
  private final Expression expr;

  public ParquetDictionaryRowGroupFilter(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public ParquetDictionaryRowGroupFilter(Schema schema, Expression unbound, boolean caseSensitive) {
    this(schema, unbound, caseSensitive, false);
  }

  public ParquetDictionaryRowGroupFilter(
      Schema schema, Expression expr, boolean caseSensitive, boolean boundAlready) {
    this.schema = schema;
    StructType struct = schema.asStruct();
    if (boundAlready) {
      this.expr = expr;
    } else {
      this.expr = Binder.bind(struct, Expressions.rewriteNot(expr), caseSensitive);
    }
  }

  /**
   * Test whether the dictionaries for a row group may contain records that match the expression.
   *
   * @param fileSchema schema for the Parquet file
   * @param dictionaries a dictionary page read store
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean shouldRead(
      MessageType fileSchema, BlockMetaData rowGroup, DictionaryPageReadStore dictionaries) {
    return new EvalVisitor().eval(fileSchema, rowGroup, dictionaries);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class EvalVisitor extends BoundExpressionVisitor<Boolean> {
    private DictionaryPageReadStore dictionaries = null;
    private Map<Integer, NavigableSet<?>> dictCache = null;
    private Map<Integer, Boolean> isFallback = null;
    private Map<Integer, Boolean> mayContainNulls = null;
    private Map<Integer, ColumnDescriptor> cols = null;
    private Map<Integer, Function<Object, Object>> conversions = null;

    private boolean eval(
        MessageType fileSchema,
        BlockMetaData rowGroup,
        DictionaryPageReadStore dictionaryReadStore) {
      this.dictionaries = dictionaryReadStore;
      this.dictCache = Maps.newHashMap();
      this.isFallback = Maps.newHashMap();
      this.mayContainNulls = Maps.newHashMap();
      this.cols = Maps.newHashMap();
      this.conversions = Maps.newHashMap();

      for (ColumnDescriptor desc : fileSchema.getColumns()) {
        PrimitiveType colType = fileSchema.getType(desc.getPath()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          Type icebergType = schema.findType(id);
          cols.put(id, desc);
          conversions.put(id, ParquetConversions.converterFromParquet(colType, icebergType));
        }
      }

      for (ColumnChunkMetaData meta : rowGroup.getColumns()) {
        PrimitiveType colType = fileSchema.getType(meta.getPath().toArray()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          isFallback.put(id, ParquetUtil.hasNonDictionaryPages(meta));
          mayContainNulls.put(id, mayContainNull(meta));
        }
      }

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return ROWS_MIGHT_MATCH; // all rows match
    }

    @Override
    public Boolean alwaysFalse() {
      return ROWS_CANNOT_MATCH; // all rows fail
    }

    @Override
    public Boolean not(Boolean result) {
      return !result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(BoundReference<T> ref) {
      // dictionaries only contain non-nulls and cannot eliminate based on isNull or NotNull
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      // dictionaries only contain non-nulls and cannot eliminate based on isNull or NotNull
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean isNaN(BoundReference<T> ref) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, comparatorForNaNPredicate(ref));
      return dictionary.stream().anyMatch(NaNUtil::isNaN) ? ROWS_MIGHT_MATCH : ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean notNaN(BoundReference<T> ref) {
      int id = ref.fieldId();

      if (mayContainNulls.get(id)) {
        return ROWS_MIGHT_MATCH;
      }

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, comparatorForNaNPredicate(ref));
      return dictionary.stream().allMatch(NaNUtil::isNaN) ? ROWS_CANNOT_MATCH : ROWS_MIGHT_MATCH;
    }

    private <T> Comparator<T> comparatorForNaNPredicate(BoundReference<T> ref) {
      // Construct the same comparator as in ComparableLiteral.comparator, ignoring null value order
      // as dictionary
      // cannot contain null values.
      // No need to check type: incompatible types will be handled during expression binding.
      return Comparators.forType(ref.type().asPrimitiveType());
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());

      // if any item in the dictionary matches the predicate, then at least one row does
      for (T item : dictionary) {
        int cmp = lit.comparator().compare(item, lit.value());
        if (cmp < 0) {
          return ROWS_MIGHT_MATCH;
        }
      }

      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());

      // if any item in the dictionary matches the predicate, then at least one row does
      for (T item : dictionary) {
        int cmp = lit.comparator().compare(item, lit.value());
        if (cmp <= 0) {
          return ROWS_MIGHT_MATCH;
        }
      }

      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());

      // if any item in the dictionary matches the predicate, then at least one row does
      for (T item : dictionary) {
        int cmp = lit.comparator().compare(item, lit.value());
        if (cmp > 0) {
          return ROWS_MIGHT_MATCH;
        }
      }

      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());

      // if any item in the dictionary matches the predicate, then at least one row does
      for (T item : dictionary) {
        int cmp = lit.comparator().compare(item, lit.value());
        if (cmp >= 0) {
          return ROWS_MIGHT_MATCH;
        }
      }

      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());

      return dictionary.contains(lit.value()) ? ROWS_MIGHT_MATCH : ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());
      if (dictionary.size() > 1 || mayContainNulls.get(id)) {
        return ROWS_MIGHT_MATCH;
      }

      return dictionary.contains(lit.value()) ? ROWS_CANNOT_MATCH : ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, ref.comparator());

      // we need to find out the smaller set to iterate through
      Set<T> smallerSet;
      Set<T> biggerSet;

      if (literalSet.size() < dictionary.size()) {
        smallerSet = literalSet;
        biggerSet = dictionary;
      } else {
        smallerSet = dictionary;
        biggerSet = literalSet;
      }

      for (T e : smallerSet) {
        if (biggerSet.contains(e)) {
          // value sets intersect so rows match
          return ROWS_MIGHT_MATCH;
        }
      }

      // value sets are disjoint so rows don't match
      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, ref.comparator());
      if (dictionary.size() > literalSet.size() || mayContainNulls.get(id)) {
        return ROWS_MIGHT_MATCH;
      }

      // ROWS_CANNOT_MATCH if no values in the dictionary that are not also in the set (the
      // difference is empty)
      return Sets.difference(dictionary, literalSet).isEmpty()
          ? ROWS_CANNOT_MATCH
          : ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean rangeIn(BoundReference<T> ref, Set<T> literalSetX) {
      NavigableSet<T> literalSet = (NavigableSet<T>) literalSetX;
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      NavigableSet<T> dictionary = dict(id, ref.comparator());

      // we need to find out the smaller set to iterate through
      NavigableSet<T> firstSet;
      NavigableSet<T> secondSet;
      if (literalSet.isEmpty() || dictionary.isEmpty()) {
        return ROWS_CANNOT_MATCH;
      }

      Comparator<? super T> comparator = literalSet.comparator();

      if (literalSet.size() < dictionary.size()) {
        firstSet = literalSet;
        secondSet = dictionary.tailSet(firstSet.first(), true);
      } else {
        firstSet = dictionary;
        secondSet = literalSet.tailSet(firstSet.first(), true);
      }

      return anyMatch(firstSet, secondSet, comparator);
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());
      for (T item : dictionary) {
        if (item.toString().startsWith(lit.value().toString())) {
          return ROWS_MIGHT_MATCH;
        }
      }

      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Boolean hasNonDictPage = isFallback.get(id);
      if (hasNonDictPage == null || hasNonDictPage) {
        return ROWS_MIGHT_MATCH;
      }

      Set<T> dictionary = dict(id, lit.comparator());
      for (T item : dictionary) {
        if (!item.toString().startsWith(lit.value().toString())) {
          return ROWS_MIGHT_MATCH;
        }
      }

      return ROWS_CANNOT_MATCH;
    }

    @SuppressWarnings("unchecked")
    private <T> NavigableSet<T> dict(int id, Comparator<T> comparator) {
      Preconditions.checkNotNull(dictionaries, "Dictionary is required");

      NavigableSet<?> cached = dictCache.get(id);
      if (cached != null) {
        return (NavigableSet<T>) cached;
      }

      ColumnDescriptor col = cols.get(id);
      DictionaryPage page = dictionaries.readDictionaryPage(col);
      // may not be dictionary-encoded
      if (page == null) {
        throw new IllegalStateException("Failed to read required dictionary page for id: " + id);
      }

      Function<Object, Object> conversion = conversions.get(id);

      Dictionary dict;
      try {
        dict = page.getEncoding().initDictionary(col, page);
      } catch (IOException e) {
        throw new RuntimeIOException("Failed to create reader for dictionary page");
      }

      NavigableSet<T> dictSet = Sets.newTreeSet(comparator);

      for (int i = 0; i <= dict.getMaxId(); i++) {
        switch (col.getPrimitiveType().getPrimitiveTypeName()) {
          case FIXED_LEN_BYTE_ARRAY:
            dictSet.add((T) conversion.apply(dict.decodeToBinary(i)));
            break;
          case BINARY:
            dictSet.add((T) conversion.apply(dict.decodeToBinary(i)));
            break;
          case INT32:
            dictSet.add((T) conversion.apply(dict.decodeToInt(i)));
            break;
          case INT64:
            dictSet.add((T) conversion.apply(dict.decodeToLong(i)));
            break;
          case FLOAT:
            dictSet.add((T) conversion.apply(dict.decodeToFloat(i)));
            break;
          case DOUBLE:
            dictSet.add((T) conversion.apply(dict.decodeToDouble(i)));
            break;
          case INT96:
            dictSet.add((T) conversion.apply(dict.decodeToBinary(i)));
            break;
          default:
            throw new IllegalArgumentException(
                "Cannot decode dictionary of type: "
                    + col.getPrimitiveType().getPrimitiveTypeName());
        }
      }

      dictCache.put(id, dictSet);

      return dictSet;
    }

    @Override
    public <T> Boolean handleNonReference(Bound<T> term) {
      return ROWS_MIGHT_MATCH;
    }
  }

  private static <T> Boolean anyMatch(
      NavigableSet<T> firstSet, NavigableSet<T> secondSet, Comparator<? super T> comparator) {
    Iterator<T> firstIter = firstSet.iterator();
    Iterator<T> secondIter = secondSet.iterator();
    boolean moveFirst = true;
    boolean moveSecond = true;
    T firstEle = null;
    T secondEle = null;
    while ((!moveFirst || firstIter.hasNext()) && (!moveSecond || secondIter.hasNext())) {
      if (moveFirst) {
        firstEle = firstIter.next();
        moveFirst = false;
      }
      if (moveSecond) {
        secondEle = secondIter.next();
        moveSecond = false;
      }
      if (firstEle != null && secondEle != null) {
        int compare = comparator.compare(firstEle, secondEle);
        if (compare == 0) {
          return ROWS_MIGHT_MATCH;
        } else if (compare < 0) {
          moveFirst = true;
        } else {
          moveSecond = true;
        }
      } else {
        moveFirst = firstEle == null;
        moveSecond = secondEle == null;
      }
    }
    // value sets are disjoint so rows don't match
    return ROWS_CANNOT_MATCH;
  }

  private static boolean mayContainNull(ColumnChunkMetaData meta) {
    return meta.getStatistics() == null || meta.getStatistics().getNumNulls() != 0;
  }
}
