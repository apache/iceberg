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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetDictionaryRowGroupFilter {
  private final Expression expr;
  private transient ThreadLocal<EvalVisitor> visitors = null;

  private EvalVisitor visitor() {
    if (visitors == null) {
      this.visitors = ThreadLocal.withInitial(EvalVisitor::new);
    }
    return visitors.get();
  }

  public ParquetDictionaryRowGroupFilter(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public ParquetDictionaryRowGroupFilter(Schema schema, Expression unbound, boolean caseSensitive) {
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, Expressions.rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether the dictionaries for a row group may contain records that match the expression.
   *
   * @param fileSchema schema for the Parquet file
   * @param dictionaries a dictionary page read store
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean shouldRead(MessageType fileSchema, BlockMetaData rowGroup,
                            DictionaryPageReadStore dictionaries) {
    return visitor().eval(fileSchema, rowGroup, dictionaries);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class EvalVisitor extends BoundExpressionVisitor<Boolean> {
    private DictionaryPageReadStore dictionaries = null;
    private Map<Integer, Set<?>> dictCache = null;
    private Map<Integer, Boolean> isFallback = null;
    private Map<Integer, Boolean> mayContainNulls = null;
    private Map<Integer, ColumnDescriptor> cols = null;
    private Map<Integer, Function<Object, Object>> conversions = null;

    private boolean eval(MessageType fileSchema, BlockMetaData rowGroup,
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
          cols.put(id, desc);
          conversions.put(id, ParquetConversions.converterFromParquet(colType));
        }
      }

      for (ColumnChunkMetaData meta : rowGroup.getColumns()) {
        PrimitiveType colType = fileSchema.getType(meta.getPath().toArray()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          isFallback.put(id, hasNonDictionaryPages(meta));
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
    public <T> Boolean in(BoundReference<T> ref, Literal<T> lit) {
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Literal<T> lit) {
      return ROWS_MIGHT_MATCH;
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

    @SuppressWarnings("unchecked")
    private <T> Set<T> dict(int id, Comparator<T> comparator) {
      Preconditions.checkNotNull(dictionaries, "Dictionary is required");

      Set<?> cached = dictCache.get(id);
      if (cached != null) {
        return (Set<T>) cached;
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

      Set<T> dictSet = Sets.newTreeSet(comparator);

      for (int i = 0; i <= dict.getMaxId(); i++) {
        switch (col.getPrimitiveType().getPrimitiveTypeName()) {
          case BINARY: dictSet.add((T) conversion.apply(dict.decodeToBinary(i)));
            break;
          case INT32: dictSet.add((T) conversion.apply(dict.decodeToInt(i)));
            break;
          case INT64: dictSet.add((T) conversion.apply(dict.decodeToLong(i)));
            break;
          case FLOAT: dictSet.add((T) conversion.apply(dict.decodeToFloat(i)));
            break;
          case DOUBLE: dictSet.add((T) conversion.apply(dict.decodeToDouble(i)));
            break;
          default:
            throw new IllegalArgumentException(
                "Cannot decode dictionary of type: " + col.getPrimitiveType().getPrimitiveTypeName());
        }
      }

      dictCache.put(id, dictSet);

      return dictSet;
    }
  }

  private static boolean mayContainNull(ColumnChunkMetaData meta) {
    return meta.getStatistics() == null || meta.getStatistics().getNumNulls() != 0;
  }

  @SuppressWarnings("deprecation")
  private static boolean hasNonDictionaryPages(ColumnChunkMetaData meta) {
    EncodingStats stats = meta.getEncodingStats();
    if (stats != null) {
      return stats.hasNonDictionaryEncodedPages();
    }

    // without EncodingStats, fall back to testing the encoding list
    Set<Encoding> encodings = new HashSet<Encoding>(meta.getEncodings());
    if (encodings.remove(Encoding.PLAIN_DICTIONARY)) {
      // if remove returned true, PLAIN_DICTIONARY was present, which means at
      // least one page was dictionary encoded and 1.0 encodings are used

      // RLE and BIT_PACKED are only used for repetition or definition levels
      encodings.remove(Encoding.RLE);
      encodings.remove(Encoding.BIT_PACKED);

      // when empty, no encodings other than dictionary or rep/def levels
      return !encodings.isEmpty();
    } else {
      // if PLAIN_DICTIONARY wasn't present, then either the column is not
      // dictionary-encoded, or the 2.0 encoding, RLE_DICTIONARY, was used.
      // for 2.0, this cannot determine whether a page fell back without
      // page encoding stats
      return true;
    }
  }
}
