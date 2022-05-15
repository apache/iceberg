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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

/**
 * A {@link Table} implementation that exposes a table's valid manifest files as rows.
 *
 * <p>A valid manifest file is one that is referenced from any snapshot currently tracked by the
 * table.
 *
 * <p>This table may return duplicate rows.
 */
public class AllManifestsTable extends BaseMetadataTable {
  public static final Types.NestedField REF_SNAPSHOT_ID =
      Types.NestedField.required(18, "reference_snapshot_id", Types.LongType.get());

  private static final Schema MANIFEST_FILE_SCHEMA =
      new Schema(
          Types.NestedField.required(14, "content", Types.IntegerType.get()),
          Types.NestedField.required(1, "path", Types.StringType.get()),
          Types.NestedField.required(2, "length", Types.LongType.get()),
          Types.NestedField.optional(3, "partition_spec_id", Types.IntegerType.get()),
          Types.NestedField.optional(4, "added_snapshot_id", Types.LongType.get()),
          Types.NestedField.optional(5, "added_data_files_count", Types.IntegerType.get()),
          Types.NestedField.optional(6, "existing_data_files_count", Types.IntegerType.get()),
          Types.NestedField.optional(7, "deleted_data_files_count", Types.IntegerType.get()),
          Types.NestedField.required(15, "added_delete_files_count", Types.IntegerType.get()),
          Types.NestedField.required(16, "existing_delete_files_count", Types.IntegerType.get()),
          Types.NestedField.required(17, "deleted_delete_files_count", Types.IntegerType.get()),
          Types.NestedField.optional(
              8,
              "partition_summaries",
              Types.ListType.ofRequired(
                  9,
                  Types.StructType.of(
                      Types.NestedField.required(10, "contains_null", Types.BooleanType.get()),
                      Types.NestedField.required(11, "contains_nan", Types.BooleanType.get()),
                      Types.NestedField.optional(12, "lower_bound", Types.StringType.get()),
                      Types.NestedField.optional(13, "upper_bound", Types.StringType.get())))),
          REF_SNAPSHOT_ID);

  AllManifestsTable(Table table) {
    this(table, table.name() + ".all_manifests");
  }

  AllManifestsTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new AllManifestsTableScan(table(), MANIFEST_FILE_SCHEMA);
  }

  @Override
  public Schema schema() {
    return MANIFEST_FILE_SCHEMA;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_MANIFESTS;
  }

  public static class AllManifestsTableScan extends BaseAllMetadataTableScan {

    AllManifestsTableScan(Table table, Schema fileSchema) {
      super(table, fileSchema, MetadataTableType.ALL_MANIFESTS);
    }

    private AllManifestsTableScan(Table table, Schema schema, TableScanContext context) {
      super(table, schema, MetadataTableType.ALL_MANIFESTS, context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new AllManifestsTableScan(table, schema, context);
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      FileIO io = table().io();
      Map<Integer, PartitionSpec> specs = Maps.newHashMap(table().specs());
      Expression filter = shouldIgnoreResiduals() ? Expressions.alwaysTrue() : filter();

      SnapshotEvaluator snapshotEvaluator =
          new SnapshotEvaluator(filter, MANIFEST_FILE_SCHEMA.asStruct(), isCaseSensitive());
      Iterable<Snapshot> filteredSnapshots =
          Iterables.filter(table().snapshots(), snapshotEvaluator::eval);

      return CloseableIterable.withNoopClose(
          Iterables.transform(
              filteredSnapshots,
              snap -> {
                if (snap.manifestListLocation() != null) {
                  return new ManifestListReadTask(
                      io, schema(), specs, snap.manifestListLocation(), filter, snap.snapshotId());
                } else {
                  return StaticDataTask.of(
                      io.newInputFile(
                          ((BaseTable) table()).operations().current().metadataFileLocation()),
                      MANIFEST_FILE_SCHEMA,
                      schema(),
                      snap.allManifests(io),
                      manifest ->
                          manifestFileToRow(
                              specs.get(manifest.partitionSpecId()), manifest, snap.snapshotId()));
                }
              }));
    }
  }

  static class ManifestListReadTask implements DataTask {
    private final FileIO io;
    private final Schema schema;
    private final Map<Integer, PartitionSpec> specs;
    private final String manifestListLocation;
    private final Expression residual;
    private final long referenceSnapshotId;
    private DataFile lazyDataFile = null;

    ManifestListReadTask(
        FileIO io,
        Schema schema,
        Map<Integer, PartitionSpec> specs,
        String manifestListLocation,
        Expression residual,
        long referenceSnapshotId) {
      this.io = io;
      this.schema = schema;
      this.specs = specs;
      this.manifestListLocation = manifestListLocation;
      this.residual = residual;
      this.referenceSnapshotId = referenceSnapshotId;
    }

    @Override
    public List<DeleteFile> deletes() {
      return ImmutableList.of();
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      try (CloseableIterable<ManifestFile> manifests =
          Avro.read(io.newInputFile(manifestListLocation))
              .rename("manifest_file", GenericManifestFile.class.getName())
              .rename("partitions", GenericPartitionFieldSummary.class.getName())
              .rename("r508", GenericPartitionFieldSummary.class.getName())
              .project(ManifestFile.schema())
              .classLoader(GenericManifestFile.class.getClassLoader())
              .reuseContainers(false)
              .build()) {

        CloseableIterable<StructLike> rowIterable =
            CloseableIterable.transform(
                manifests,
                manifest ->
                    manifestFileToRow(
                        specs.get(manifest.partitionSpecId()), manifest, referenceSnapshotId));

        StructProjection projection = StructProjection.create(MANIFEST_FILE_SCHEMA, schema);
        return CloseableIterable.transform(rowIterable, projection::wrap);

      } catch (IOException e) {
        throw new UncheckedIOException(
            String.format("Cannot read manifest list file: %s", manifestListLocation), e);
      }
    }

    @Override
    public DataFile file() {
      if (lazyDataFile == null) {
        this.lazyDataFile =
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(io.newInputFile(manifestListLocation))
                .withRecordCount(1)
                .withFormat(FileFormat.AVRO)
                .build();
      }

      return lazyDataFile;
    }

    @Override
    public PartitionSpec spec() {
      return PartitionSpec.unpartitioned();
    }

    @Override
    public long start() {
      return 0;
    }

    @Override
    public long length() {
      // return a generic length to avoid looking up the actual length
      return 8192;
    }

    @Override
    public Expression residual() {
      // this table is unpartitioned so the residual is always constant
      return residual;
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }
  }

  static StaticDataTask.Row manifestFileToRow(
      PartitionSpec spec, ManifestFile manifest, long referenceSnapshotId) {
    return StaticDataTask.Row.of(
        manifest.content().id(),
        manifest.path(),
        manifest.length(),
        manifest.partitionSpecId(),
        manifest.snapshotId(),
        manifest.content() == ManifestContent.DATA ? manifest.addedFilesCount() : 0,
        manifest.content() == ManifestContent.DATA ? manifest.existingFilesCount() : 0,
        manifest.content() == ManifestContent.DATA ? manifest.deletedFilesCount() : 0,
        manifest.content() == ManifestContent.DELETES ? manifest.addedFilesCount() : 0,
        manifest.content() == ManifestContent.DELETES ? manifest.existingFilesCount() : 0,
        manifest.content() == ManifestContent.DELETES ? manifest.deletedFilesCount() : 0,
        ManifestsTable.partitionSummariesToRows(spec, manifest.partitions()),
        referenceSnapshotId);
  }

  private static class SnapshotEvaluator {

    private final Expression boundExpr;

    private SnapshotEvaluator(Expression expr, Types.StructType structType, boolean caseSensitive) {
      this.boundExpr = Binder.bind(structType, expr, caseSensitive);
    }

    private boolean eval(Snapshot snapshot) {
      return new SnapshotEvalVisitor().eval(snapshot);
    }

    private class SnapshotEvalVisitor extends BoundExpressionVisitor<Boolean> {

      private long snapshotId;
      private static final boolean ROWS_MIGHT_MATCH = true;
      private static final boolean ROWS_CANNOT_MATCH = false;

      private boolean eval(Snapshot snapshot) {
        this.snapshotId = snapshot.snapshotId();
        return ExpressionVisitors.visitEvaluator(boundExpr, this);
      }

      @Override
      public Boolean alwaysTrue() {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public Boolean alwaysFalse() {
        return ROWS_CANNOT_MATCH;
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
        if (isSnapshotRef(ref)) {
          return ROWS_CANNOT_MATCH; // reference_snapshot_id is never null
        } else {
          return ROWS_MIGHT_MATCH;
        }
      }

      @Override
      public <T> Boolean notNull(BoundReference<T> ref) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean isNaN(BoundReference<T> ref) {
        if (isSnapshotRef(ref)) {
          return ROWS_CANNOT_MATCH; // reference_snapshot_id is never nan
        } else {
          return ROWS_MIGHT_MATCH;
        }
      }

      @Override
      public <T> Boolean notNaN(BoundReference<T> ref) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
        return compareSnapshotRef(ref, lit, compareResult -> compareResult < 0);
      }

      @Override
      public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
        return compareSnapshotRef(ref, lit, compareResult -> compareResult <= 0);
      }

      @Override
      public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
        return compareSnapshotRef(ref, lit, compareResult -> compareResult > 0);
      }

      @Override
      public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
        return compareSnapshotRef(ref, lit, compareResult -> compareResult >= 0);
      }

      @Override
      public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
        return compareSnapshotRef(ref, lit, compareResult -> compareResult == 0);
      }

      @Override
      public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
        return compareSnapshotRef(ref, lit, compareResult -> compareResult != 0);
      }

      @Override
      public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
        if (isSnapshotRef(ref)) {
          if (!literalSet.contains(snapshotId)) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
        if (isSnapshotRef(ref)) {
          if (literalSet.contains(snapshotId)) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      /**
       * Comparison of snapshot reference and literal, using long comparator.
       *
       * @param ref bound reference, comparison attempted only if reference is for
       *     reference_snapshot_id
       * @param lit literal value to compare with snapshot id.
       * @param desiredResult function to apply to long comparator result, returns true if result is
       *     as expected.
       * @return false if comparator does not achieve desired result, true otherwise
       */
      private <T> Boolean compareSnapshotRef(
          BoundReference<T> ref, Literal<T> lit, Function<Integer, Boolean> desiredResult) {
        if (isSnapshotRef(ref)) {
          Literal<Long> longLit = lit.to(Types.LongType.get());
          int cmp = longLit.comparator().compare(snapshotId, longLit.value());
          if (!desiredResult.apply(cmp)) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      private <T> boolean isSnapshotRef(BoundReference<T> ref) {
        return ref.fieldId() == REF_SNAPSHOT_ID.fieldId();
      }
    }
  }
}
