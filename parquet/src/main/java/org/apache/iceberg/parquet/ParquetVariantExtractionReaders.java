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

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.parquet.ParquetVariantReaders.VariantValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

/**
 * Selective Parquet readers for shredded variant extraction paths.
 *
 * <p>A shredded variant stores known fields in separate typed Parquet columns alongside a {@code
 * value} binary blob that holds anything not shredded. This class reads one or more specific paths
 * (e.g. {@code $.user.name}, {@code $[0]}) from a shredded variant column while touching only the
 * Parquet columns relevant to those paths.
 *
 * <h2>Reader selection strategy (per field path)</h2>
 *
 * <ol>
 *   <li><b>Selective spine</b> — for 2+ part object paths whose top key is shredded, a chain of
 *       {@code SelectiveObjectReader} instances walk only the {@code value} blob and the single
 *       child column subtree at each level, skipping all siblings.
 *   <li><b>Shredded leaf</b> — for paths that resolve directly to a shredded typed or serialized
 *       column, a {@code RowCachedReader} reads that column once per row and caches the result so
 *       multiple fields sharing the same path (e.g. {@code $.size} as {@code double} AND as {@code
 *       long}) do not re-read the physical column.
 *   <li><b>Array path</b> — for paths starting with an array index (e.g. {@code $[0]}), the full
 *       shredded root value is reconstructed once and the index is applied in memory.
 *   <li><b>Root fallback</b> — for unshredded paths, the root serialized {@code value} blob is read
 *       once per row (shared across all fallback fields) and the path is applied in memory.
 *   <li><b>Null</b> — paths that cannot be resolved in the file schema return {@code null}.
 * </ol>
 *
 * <h2>Column deduplication</h2>
 *
 * {@link VariantExtractionRowReader} collects all {@link TripleIterator} column cursors via
 * identity-based deduplication ({@link java.util.IdentityHashMap}). This is necessary because
 * multiple field readers may reference the exact same underlying physical column object; advancing
 * it twice per row would corrupt the read stream.
 */
public class ParquetVariantExtractionReaders {
  private ParquetVariantExtractionReaders() {}

  public static int leafColumnCount(ParquetValueReader<?> reader) {
    return reader.columns().size();
  }

  public static ParquetValueReader<VariantExtractionRow> buildRowReader(
      MessageType fileSchema,
      GroupType variantGroup,
      List<String> variantColumnPath,
      List<VariantExtractionField> fields) {
    return new VariantExtractionRowReader(fileSchema, variantGroup, variantColumnPath, fields);
  }

  /**
   * The result of reading one Parquet row through a {@link ParquetVariantExtractionReaders} reader.
   * Holds the row's variant metadata, one {@link VariantValue} per requested field (indexed by
   * {@link VariantExtractionField#ordinal()}), and a placeholder flag for fields that were declared
   * as placeholders rather than real extraction targets. All value slots are {@code null} when the
   * variant column itself is SQL NULL.
   */
  public static final class VariantExtractionRow {
    private VariantMetadata metadata;
    private final VariantValue[] values;
    private final boolean[] placeholders;

    private VariantExtractionRow(
        VariantMetadata metadata, VariantValue[] values, boolean[] placeholders) {
      this.metadata = metadata;
      this.values = values;
      this.placeholders = placeholders;
    }

    /**
     * Returns the variant metadata for this row, or {@code null} when the Parquet variant column is
     * SQL NULL (the shredded {@code metadata} column is absent at the current definition level).
     */
    public VariantMetadata metadata() {
      return metadata;
    }

    public VariantValue value(int ordinal) {
      return values[ordinal];
    }

    public boolean placeholder(int ordinal) {
      return placeholders[ordinal];
    }

    public int numFields() {
      return values.length;
    }
  }

  /**
   * Describes one field to extract from a shredded variant column. Each field has a unique {@code
   * ordinal} (its slot index in the output row), a parsed {@code pathParts} list (from a JSON path
   * like {@code $.user.name} or {@code $[0]}), and an optional {@code placeholder} flag for
   * ordinals that should be filled with a sentinel rather than a real value.
   */
  public static final class VariantExtractionField {
    private final int ordinal;
    private final boolean placeholder;
    private final List<PathUtil.PathSegment> pathParts;

    public VariantExtractionField(
        int ordinal, boolean placeholder, List<PathUtil.PathSegment> pathParts) {
      this.ordinal = ordinal;
      this.placeholder = placeholder;
      this.pathParts = pathParts;
    }

    public int ordinal() {
      return ordinal;
    }

    public boolean placeholder() {
      return placeholder;
    }

    public List<PathUtil.PathSegment> pathParts() {
      return pathParts;
    }
  }

  /**
   * Orchestrates reading one Parquet row into a {@link VariantExtractionRow}. Builds one {@link
   * FieldValueReader} per requested field during construction using the reader selection strategy
   * described in {@link ParquetVariantExtractionReaders}. The {@code rowPosition} counter
   * invalidates per-row caches in {@link RowCachedReader} instances; {@code rootValuePosition}
   * tracks lazy advancement of the root fallback column so it is consumed exactly once per row even
   * when multiple fields need it.
   */
  private static class VariantExtractionRowReader
      implements ParquetValueReader<VariantExtractionRow> {
    private final int numFields;
    private final FieldSpec[] fields;
    private final ParquetValueReader<VariantMetadata> metadataReader;
    private final VariantValueReader rootFallbackReader;
    private final int rootValueDefinitionLevel;
    private final TripleIterator<?> column;
    private final List<TripleIterator<?>> columnCursors;
    private final Map<List<PathUtil.PathSegment>, RowCachedReader> rowCachedReaders =
        Maps.newHashMap();
    private long rowPosition = 0L;
    private long rootValuePosition = 0L;

    private VariantExtractionRowReader(
        MessageType fileSchema,
        GroupType variantGroup,
        List<String> variantColumnPath,
        List<VariantExtractionField> extractionFields) {
      this.numFields =
          extractionFields.stream().mapToInt(VariantExtractionField::ordinal).max().orElse(-1) + 1;
      this.fields = new FieldSpec[numFields];
      for (VariantExtractionField field : extractionFields) {
        fields[field.ordinal()] = new FieldSpec(field);
      }

      ColumnDescriptor metadataDesc =
          fileSchema.getColumnDescription(
              VariantExtractionPathResolver.pathArray(variantColumnPath, "metadata"));
      this.metadataReader = ParquetVariantReaders.metadata(fileSchema, metadataDesc);

      for (FieldSpec field : fields) {
        if (field != null && !field.placeholder()) {
          field.reader = buildFieldReader(fileSchema, variantGroup, variantColumnPath, field);
        }
      }

      boolean needsRootFallback =
          Arrays.stream(fields).filter(Objects::nonNull).anyMatch(field -> field.needsRootFallback);

      if (needsRootFallback && VariantExtractionPathResolver.hasRootSerializedValue(variantGroup)) {
        ColumnDescriptor valueDesc =
            fileSchema.getColumnDescription(
                VariantExtractionPathResolver.pathArray(variantColumnPath, "value"));
        this.rootValueDefinitionLevel = fileSchema.getMaxDefinitionLevel(valueDesc.getPath()) - 1;
        this.rootFallbackReader = ParquetVariantReaders.serialized(valueDesc);
      } else {
        this.rootValueDefinitionLevel = Integer.MAX_VALUE;
        this.rootFallbackReader = null;
      }

      this.column = metadataReader.column();
      this.columnCursors = collectColumnCursors();
    }

    private List<TripleIterator<?>> collectColumnCursors() {
      ImmutableList.Builder<TripleIterator<?>> columns = ImmutableList.builder();
      Set<TripleIterator<?>> seenColumns = Collections.newSetFromMap(new IdentityHashMap<>());
      addColumnCursors(columns, seenColumns, metadataReader.columns());
      if (rootFallbackReader != null) {
        addColumnCursors(columns, seenColumns, rootFallbackReader.columns());
      }

      for (FieldSpec field : fields) {
        if (field != null && field.reader != null) {
          addColumnCursors(columns, seenColumns, field.reader.columns());
        }
      }

      return columns.build();
    }

    private void addColumnCursors(
        ImmutableList.Builder<TripleIterator<?>> columns,
        Set<TripleIterator<?>> seenColumns,
        Iterable<TripleIterator<?>> columnsToAdd) {
      for (TripleIterator<?> columnToAdd : columnsToAdd) {
        if (seenColumns.add(columnToAdd)) {
          columns.add(columnToAdd);
        }
      }
    }

    private FieldValueReader buildFieldReader(
        MessageType fileSchema,
        GroupType variantGroup,
        List<String> variantColumnPath,
        FieldSpec field) {
      FieldValueReader reader =
          buildSelectiveNestedReader(fileSchema, variantGroup, variantColumnPath, field);
      if (reader != null) {
        return reader;
      }
      reader = buildShreddedLeafReader(fileSchema, variantGroup, variantColumnPath, field);
      if (reader != null) {
        return reader;
      }
      reader = buildArrayPathReader(fileSchema, variantGroup, variantColumnPath, field);
      if (reader != null) {
        return reader;
      }
      if (VariantExtractionPathResolver.hasRootSerializedValue(variantGroup)) {
        field.needsRootFallback = true;
      }
      return null;
    }

    /**
     * Builds a {@link SelectiveObjectReader} chain for a 2+ part object path whose top key is
     * shredded. Each level reads only the {@code value} blob plus the single child subtree toward
     * the target leaf, avoiding reconstruction of all siblings.
     */
    private FieldValueReader buildSelectiveNestedReader(
        MessageType fileSchema,
        GroupType variantGroup,
        List<String> variantColumnPath,
        FieldSpec field) {
      if (field.pathParts().size() <= 1
          || field.pathParts().get(0) instanceof PathUtil.PathSegment.Index) {
        return null;
      }
      String topKey = ((PathUtil.PathSegment.Name) field.pathParts().get(0)).name();
      GroupType topGroup =
          VariantExtractionPathResolver.resolveShreddedFieldGroup(
              variantGroup, ImmutableList.of(new PathUtil.PathSegment.Name(topKey)));
      if (topGroup == null) {
        return null;
      }
      List<String> topPath =
          VariantExtractionPathResolver.readerPath(
              variantColumnPath, ImmutableList.of(new PathUtil.PathSegment.Name(topKey)));
      List<PathUtil.PathSegment> remainingPath =
          field.pathParts().subList(1, field.pathParts().size());
      ParquetVariantReaders.VariantValueReader selective =
          buildSelectivePathReader(fileSchema, topGroup, topPath, remainingPath);
      if (selective == null) {
        return null;
      }
      // selective reads from topGroup and returns the topKey-level VariantValue — a partial
      // object including only the remainingPath child (e.g. $.a.b.c returns {b:{c:leaf}}).
      // PathNavigatingFieldReader then applies remainingPath in-memory to extract the leaf.
      RowCachedReader sharedReader =
          rowCachedReaders.computeIfAbsent(field.pathParts(), k -> new RowCachedReader(selective));
      return new PathNavigatingFieldReader(sharedReader, remainingPath);
    }

    /**
     * Builds a reader for a path whose final hop is a directly shredded typed or serialized leaf.
     * The underlying column reader is shared across all fields with the same path (e.g. {@code
     * $.size} as {@code double} AND as {@code long}); the Spark-level cast handles per-ordinal type
     * conversion.
     */
    private FieldValueReader buildShreddedLeafReader(
        MessageType fileSchema,
        GroupType variantGroup,
        List<String> variantColumnPath,
        FieldSpec field) {
      if (field.pathParts().isEmpty()) {
        // Root extraction ($): reconstruct the full shredded variant via buildRowCachedReader,
        // which keys on the empty path. All root-reconstructing readers (this $ path and every
        // array-index path $[n] from buildArrayPathReader) share that one cached reader, so the
        // full root value is read and rebuilt only once per row regardless of how many such
        // fields reference the same column.
        RowCachedReader cachedReader =
            buildRowCachedReader(fileSchema, variantGroup, variantColumnPath);
        return new PathNavigatingFieldReader(cachedReader, ImmutableList.of());
      }

      GroupType fieldGroup =
          VariantExtractionPathResolver.resolveShreddedFieldGroup(variantGroup, field.pathParts());
      if (fieldGroup == null
          || (!VariantExtractionPathResolver.hasTypedValue(fieldGroup)
              && !VariantExtractionPathResolver.hasSerializedValue(fieldGroup))) {
        return null;
      }
      RowCachedReader shared =
          rowCachedReaders.computeIfAbsent(
              field.pathParts(),
              k -> {
                VariantValueReader reader;
                if (VariantExtractionPathResolver.hasTypedValue(fieldGroup)) {
                  List<String> readerPath =
                      VariantExtractionPathResolver.readerPath(
                          variantColumnPath, field.pathParts());
                  reader =
                      (VariantValueReader)
                          ParquetVariantVisitor.visitShreddedValueGroup(
                              fieldGroup, new VariantReaderBuilder(fileSchema, readerPath));
                } else {
                  ColumnDescriptor valueDesc =
                      fileSchema.getColumnDescription(
                          VariantExtractionPathResolver.pathToSerializedField(
                              variantColumnPath, field.pathParts()));
                  reader = ParquetVariantReaders.serialized(valueDesc);
                }
                return new RowCachedReader(reader);
              });
      return new PathNavigatingFieldReader(shared, ImmutableList.of());
    }

    /**
     * Builds a reader for paths starting with an array index (e.g. {@code $[0]}). Root arrays
     * cannot use an object-field prefix, so the full shredded root value is reconstructed and the
     * array index path is applied in memory.
     */
    private FieldValueReader buildArrayPathReader(
        MessageType fileSchema,
        GroupType variantGroup,
        List<String> variantColumnPath,
        FieldSpec field) {
      if (field.pathParts().isEmpty()
          || !(field.pathParts().get(0) instanceof PathUtil.PathSegment.Index)) {
        return null;
      }
      RowCachedReader cachedReader =
          buildRowCachedReader(fileSchema, variantGroup, variantColumnPath);
      return new PathNavigatingFieldReader(cachedReader, field.pathParts());
    }

    // Reconstructs the full root variant value, cached per row. Keyed on the empty path so that all
    // callers needing the whole root value ($ root extraction and every $[n] array-index path)
    // share one reader and read/rebuild the root value only once per row.
    private RowCachedReader buildRowCachedReader(
        MessageType fileSchema, GroupType variantGroup, List<String> readerPath) {
      return rowCachedReaders.computeIfAbsent(
          Collections.emptyList(),
          ignored -> {
            VariantValueReader reader =
                (VariantValueReader)
                    ParquetVariantVisitor.visitShreddedValueGroup(
                        variantGroup, new VariantReaderBuilder(fileSchema, readerPath));
            return new RowCachedReader(reader);
          });
    }

    /**
     * Recursively builds a selective {@link ParquetVariantReaders.VariantValueReader} for {@code
     * pathParts} starting at {@code currentGroup}.
     *
     * <p>Object hops compose a chain of {@link SelectiveObjectReader} instances (one child field
     * per level, plus that level's {@code value} blob). The leaf is a full shredded-value reader
     * from {@link ParquetVariantVisitor#visitShreddedValueGroup}. Unresolvable steps fall back to a
     * value-blob reader or full group reconstruction; array-index steps use full reconstruction.
     * Returns {@code null} when the path cannot be resolved.
     *
     * @param currentGroup shredded value group at this level ({@code value} / {@code typed_value})
     * @param currentPath Parquet column path to {@code currentGroup}
     * @param pathParts object-field segments to follow from {@code currentGroup}
     */
    private ParquetVariantReaders.VariantValueReader buildSelectivePathReader(
        MessageType fileSchema,
        GroupType currentGroup,
        List<String> currentPath,
        List<PathUtil.PathSegment> pathParts) {
      // Base case: leaf reached or array index step — build a full reader for this group.
      if (pathParts.isEmpty() || pathParts.get(0) instanceof PathUtil.PathSegment.Index) {
        return (ParquetVariantReaders.VariantValueReader)
            ParquetVariantVisitor.visitShreddedValueGroup(
                currentGroup, new VariantReaderBuilder(fileSchema, currentPath));
      }

      String fieldKey = ((PathUtil.PathSegment.Name) pathParts.get(0)).name();
      List<PathUtil.PathSegment> remainingPath = pathParts.subList(1, pathParts.size());

      // Build a reader for the serialized value blob at this level — used when typed_value absent.
      ParquetVariantReaders.VariantValueReader valueReader = null;
      int valueDL = Integer.MAX_VALUE;
      if (ParquetSchemaUtil.hasField(currentGroup, "value")) {
        ColumnDescriptor valueDesc =
            fileSchema.getColumnDescription(
                concat(currentPath, ImmutableList.of("value")).toArray(String[]::new));
        valueReader = ParquetVariantReaders.serialized(valueDesc);
        valueDL = valueDesc.getMaxDefinitionLevel() - 1;
      }

      // If typed_value is absent or a primitive, there are no shredded children to recurse into.
      if (!ParquetSchemaUtil.hasField(currentGroup, "typed_value")
          || currentGroup.getType("typed_value").isPrimitive()) {
        return valueBlobOnlyReader(valueReader, valueDL);
      }

      // If the next path key is not shredded in typed_value, fall back to the value blob.
      GroupType typedGroup = currentGroup.getType("typed_value").asGroupType();
      if (!ParquetSchemaUtil.hasField(typedGroup, fieldKey)) {
        return valueBlobOnlyReader(valueReader, valueDL);
      }

      // Recurse into the shredded child group for fieldKey.
      int fieldsDL =
          fileSchema.getMaxDefinitionLevel(
                  concat(currentPath, ImmutableList.of("typed_value")).toArray(String[]::new))
              - 1;
      GroupType fieldGroup = typedGroup.getType(fieldKey).asGroupType();
      List<String> fieldPath =
          concat(concat(currentPath, ImmutableList.of("typed_value")), ImmutableList.of(fieldKey));
      ParquetVariantReaders.VariantValueReader childReader =
          buildSelectivePathReader(fileSchema, fieldGroup, fieldPath, remainingPath);

      if (childReader == null) {
        // Child path unresolvable — fall back to full reconstruction at this level.
        return (ParquetVariantReaders.VariantValueReader)
            ParquetVariantVisitor.visitShreddedValueGroup(
                currentGroup, new VariantReaderBuilder(fileSchema, currentPath));
      }

      return new SelectiveObjectReader(valueReader, valueDL, fieldsDL, fieldKey, childReader);
    }

    private static ParquetVariantReaders.VariantValueReader valueBlobOnlyReader(
        ParquetVariantReaders.VariantValueReader valueReader, int valueDL) {
      return valueReader != null
          ? ParquetVariantReaders.shredded(valueDL, valueReader, Integer.MAX_VALUE, null)
          : null;
    }

    private static List<String> concat(List<String> base, List<String> suffix) {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      builder.addAll(base);
      builder.addAll(suffix);
      return builder.build();
    }

    @Override
    public VariantExtractionRow read(VariantExtractionRow reuse) {
      VariantMetadata metadata = metadataReader.read(null);

      VariantExtractionRow result;
      if (reuse != null && reuse.numFields() == numFields) {
        result = reuse;
        Arrays.fill(result.values, null);
        Arrays.fill(result.placeholders, false);
        result.metadata = metadata;
      } else {
        result =
            new VariantExtractionRow(metadata, new VariantValue[numFields], new boolean[numFields]);
      }

      if (metadata == null) {
        // SQL NULL variant column: shredded metadata is absent. Skip field readers and leave
        // values/placeholders unset so callers materialize an all-null extraction row.
        return result;
      }

      VariantValue[] values = result.values;
      boolean[] placeholders = result.placeholders;
      VariantValue rootValue = null;
      boolean rootValueRead = false;
      long currentRowPosition = rowPosition;
      rowPosition += 1;

      for (FieldSpec field : fields) {
        if (field == null) {
          continue;
        }

        if (field.placeholder()) {
          placeholders[field.ordinal()] = true;
        } else if (field.needsRootFallback) {
          if (!rootValueRead) {
            rootValue = readRootValue(metadata, currentRowPosition);
            rootValueRead = true;
          }

          values[field.ordinal()] = extractPath(rootValue, field.pathParts());
        } else if (field.reader != null) {
          values[field.ordinal()] = field.reader.read(metadata, currentRowPosition);
        }
      }

      return result;
    }

    // Reads the root-level fallback value for the current row, advancing the root fallback
    // column cursors exactly once. Rows where no fallback field was requested do not call this
    // method, so the cursors may have fallen behind; skipRootValue() catches them up.
    private VariantValue readRootValue(VariantMetadata metadata, long currentRowPosition) {
      if (rootFallbackReader == null) {
        return null;
      }

      // Catch up: advance past rows where no fallback field was requested (so those rows' cursors
      // were never consumed).
      while (rootValuePosition < currentRowPosition) {
        skipRootValue();
        rootValuePosition += 1;
      }

      rootValuePosition += 1;
      if (rootFallbackReader.column().currentDefinitionLevel() > rootValueDefinitionLevel) {
        return rootFallbackReader.read(metadata);
      }

      for (TripleIterator<?> child : rootFallbackReader.columns()) {
        child.nextNull();
      }

      return null;
    }

    // Advances root fallback column cursors for one row without consuming the value.
    private void skipRootValue() {
      if (rootFallbackReader.column().currentDefinitionLevel() > rootValueDefinitionLevel) {
        rootFallbackReader.column().nextBinary();
      } else {
        for (TripleIterator<?> child : rootFallbackReader.columns()) {
          child.nextNull();
        }
      }
    }

    @Override
    public TripleIterator<?> column() {
      return column;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return columnCursors;
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      metadataReader.setPageSource(pageStore);
      if (rootFallbackReader != null) {
        rootFallbackReader.setPageSource(pageStore);
      }
      this.rowPosition = 0L;
      this.rootValuePosition = 0L;

      // Call setPageSource on each RowCachedReader exactly once.
      // All PathNavigatingFieldReaders reference readers from this map, so iterating
      // the map avoids double-initialization when multiple fields share the same reader.
      rowCachedReaders.values().forEach(r -> r.setPageSource(pageStore));
    }
  }

  private static class FieldSpec {
    private final VariantExtractionField field;
    private FieldValueReader reader;
    private boolean needsRootFallback;

    private FieldSpec(VariantExtractionField field) {
      this.field = field;
    }

    private int ordinal() {
      return field.ordinal();
    }

    private boolean placeholder() {
      return field.placeholder();
    }

    private List<PathUtil.PathSegment> pathParts() {
      return field.pathParts();
    }
  }

  private interface FieldValueReader {
    VariantValue read(VariantMetadata metadata, long rowPosition);

    List<TripleIterator<?>> columns();
  }

  private static class RowCachedReader {
    private final VariantValueReader delegate;
    private long cachedRowPosition = -1L;
    private VariantValue cachedValue = null;

    private RowCachedReader(VariantValueReader delegate) {
      this.delegate = delegate;
    }

    private VariantValue read(VariantMetadata metadata, long rowPosition) {
      if (cachedRowPosition != rowPosition) {
        cachedValue = delegate.read(metadata);
        cachedRowPosition = rowPosition;
      }

      return cachedValue;
    }

    private List<TripleIterator<?>> columns() {
      return delegate.columns();
    }

    private void setPageSource(PageReadStore pageStore) {
      delegate.setPageSource(pageStore);
      cachedRowPosition = -1L;
      cachedValue = null;
    }
  }

  private static class PathNavigatingFieldReader implements FieldValueReader {
    private final RowCachedReader cachedReader;
    private final List<PathUtil.PathSegment> remainingPath;

    private PathNavigatingFieldReader(
        RowCachedReader cachedReader, List<PathUtil.PathSegment> remainingPath) {
      this.cachedReader = cachedReader;
      this.remainingPath = remainingPath;
    }

    @Override
    public VariantValue read(VariantMetadata metadata, long rowPosition) {
      VariantValue value = cachedReader.read(metadata, rowPosition);
      return value != null ? extractPath(value, remainingPath) : null;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return cachedReader.columns();
    }
  }

  /**
   * Reads exactly one child field from a shredded object variant, touching only path-relevant
   * columns. Unlike {@link RowCachedReader} wrapping a full {@link
   * ParquetVariantReaders.ShreddedObjectReader} (which reconstructs every sibling field), this
   * reader includes only:
   *
   * <ol>
   *   <li>the {@code value} blob at this level (for leftover/fallback data), and
   *   <li>the columns of the single requested child subtree.
   * </ol>
   *
   * <p>Per-row: if the probe column's definition level indicates the typed object is present, the
   * child field is read and merged with any leftover {@code value} data into a partial {@link
   * ShreddedObject}. Otherwise the {@code value} blob is returned directly.
   */
  private static class SelectiveObjectReader implements ParquetVariantReaders.VariantValueReader {
    private final ParquetVariantReaders.VariantValueReader valueReader;
    private final int valueDefinitionLevel;
    private final int fieldsDefinitionLevel;
    private final String childFieldName;
    private final ParquetVariantReaders.VariantValueReader childReader;
    private final TripleIterator<?> probeColumn;
    private final List<TripleIterator<?>> allColumns;

    private SelectiveObjectReader(
        ParquetVariantReaders.VariantValueReader valueReader,
        int valueDefinitionLevel,
        int fieldsDefinitionLevel,
        String childFieldName,
        ParquetVariantReaders.VariantValueReader childReader) {
      this.valueReader = valueReader;
      this.valueDefinitionLevel = valueDefinitionLevel;
      this.fieldsDefinitionLevel = fieldsDefinitionLevel;
      this.childFieldName = childFieldName;
      this.childReader = childReader;
      // Deepest leaf column propagated up from child: present whenever parent typed_value present.
      this.probeColumn = childReader.column();
      ImmutableList.Builder<TripleIterator<?>> cols = ImmutableList.builder();
      if (valueReader != null) {
        cols.addAll(valueReader.columns());
      }
      cols.addAll(childReader.columns());
      this.allColumns = cols.build();
    }

    // Returns true when this level's {@code typed_value} group is present for the current row.
    // This is key to determine if we should read the child value or not.
    private boolean isTypedValuePresent() {
      return probeColumn.currentDefinitionLevel() > fieldsDefinitionLevel;
    }

    @Override
    public VariantValue read(VariantMetadata metadata) {
      // Advance value column: read if present, else skip.
      VariantValue value = null;
      if (valueReader != null) {
        if (valueReader.column().currentDefinitionLevel() > valueDefinitionLevel) {
          value = valueReader.read(metadata);
        } else {
          for (TripleIterator<?> c : valueReader.columns()) {
            c.nextNull();
          }
        }
      }

      if (isTypedValuePresent()) {
        // recursively read the child value and overlay it on the value blob
        VariantValue childValue = childReader.read(metadata);
        ShreddedObject object =
            (value != null && value.type() == PhysicalType.OBJECT)
                ? Variants.object(metadata, (VariantObject) value)
                : Variants.object(metadata);
        if (childValue != null) {
          // Include variant NULL values — JSON null is a valid field value, not "missing".
          object.put(childFieldName, childValue);
        }
        return object;
      } else {
        // typed_value null: advance child columns and return serialized value blob.
        for (TripleIterator<?> c : childReader.columns()) {
          c.nextNull();
        }
        return value;
      }
    }

    @Override
    public TripleIterator<?> column() {
      // Return the deepest probe column so parent readers can check our presence correctly.
      return probeColumn;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return allColumns;
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      if (valueReader != null) {
        valueReader.setPageSource(pageStore);
      }
      childReader.setPageSource(pageStore);
    }
  }

  private static VariantValue extractPath(
      VariantValue value, List<PathUtil.PathSegment> pathParts) {
    VariantValue current = value;
    for (PathUtil.PathSegment segment : pathParts) {
      if (current == null || current.type() == PhysicalType.NULL) {
        return null;
      }

      if (segment instanceof PathUtil.PathSegment.Index) {
        if (current.type() != PhysicalType.ARRAY) {
          return null;
        }
        int index = ((PathUtil.PathSegment.Index) segment).index();
        VariantArray array = current.asArray();
        if (index >= array.numElements()) {
          return null;
        }
        current = array.get(index);
      } else {
        if (current.type() != PhysicalType.OBJECT) {
          return null;
        }
        current = current.asObject().get(((PathUtil.PathSegment.Name) segment).name());
      }
    }

    return current;
  }
}
