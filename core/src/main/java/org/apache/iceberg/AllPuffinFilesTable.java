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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Bound;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructProjection;

/**
 * A {@link Table} implementation that exposes Puffin files associated with all snapshots currently
 * tracked by a table.
 *
 * <p>Each row represents a Puffin file referenced by one snapshot through one metadata source. The
 * same physical Puffin file may appear in multiple rows when it is referenced by multiple snapshots
 * or through multiple sources.
 *
 * <p>Deletion vector files are reconstructed from live delete-manifest entries for each reference
 * snapshot. Statistics files come from the statistics files registered in the current table
 * metadata. Statistics registration can change independently of snapshots, so this table does not
 * reconstruct historical statistics registration state.
 */
public class AllPuffinFilesTable extends BaseMetadataTable {

  private static final String SOURCE_STATISTICS = "statistics";
  private static final String SOURCE_DELETION_VECTOR = "deletion_vector";

  private static final int MIN_FORMAT_VERSION_DELETION_VECTORS = 3;

  private static final List<String> DV_REFERENCED_BLOB_TYPES =
      ImmutableList.of(StandardBlobTypes.DV_V1);

  private static final List<Integer> DV_REFERENCED_FIELD_IDS =
      ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId());

  private static final List<String> DV_SCAN_COLUMNS =
      ImmutableList.of(
          DataFile.FILE_PATH.name(),
          DataFile.FILE_FORMAT.name(),
          DataFile.FILE_SIZE.name(),
          DataFile.CONTENT_OFFSET.name(),
          DataFile.CONTENT_SIZE.name());

  private static final Types.NestedField REFERENCE_SNAPSHOT_ID =
      Types.NestedField.required(
          1,
          "reference_snapshot_id",
          Types.LongType.get(),
          "ID of the snapshot that references the Puffin file");

  private static final Types.NestedField SOURCE =
      Types.NestedField.required(
          3,
          "source",
          Types.StringType.get(),
          "Metadata source that associates the Puffin file with the reference snapshot");

  private static final Schema ALL_PUFFIN_FILES_SCHEMA =
      new Schema(
          REFERENCE_SNAPSHOT_ID,
          Types.NestedField.required(
              2,
              "file_path",
              Types.StringType.get(),
              "Fully qualified location of the Puffin file"),
          SOURCE,
          Types.NestedField.required(
              4,
              "file_size_in_bytes",
              Types.LongType.get(),
              "Total size of the Puffin file in bytes"),
          Types.NestedField.required(
              5,
              "referenced_blob_count",
              Types.IntegerType.get(),
              "Number of referenced Puffin blobs represented by this row"),
          Types.NestedField.required(
              6,
              "referenced_blob_types",
              Types.ListType.ofRequired(7, Types.StringType.get()),
              "Distinct blob types across the referenced blobs"),
          Types.NestedField.required(
              8,
              "referenced_fields",
              Types.ListType.ofRequired(
                  9,
                  Types.StructType.of(
                      Types.NestedField.required(
                          10,
                          "field_id",
                          Types.IntegerType.get(),
                          "Field ID referenced by at least one blob"),
                      Types.NestedField.optional(
                          11,
                          "current_field_name",
                          Types.StringType.get(),
                          "Name currently assigned to the field ID; null if no longer present"))),
              "Distinct fields referenced across the blobs"));

  AllPuffinFilesTable(Table table) {
    this(table, table.name() + ".all_puffin_files");
  }

  AllPuffinFilesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new AllPuffinFilesTableScan(table(), schema());
  }

  @Override
  public Schema schema() {
    return ALL_PUFFIN_FILES_SCHEMA;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_PUFFIN_FILES;
  }

  private static Map<Long, List<StaticDataTask.Row>> statisticsRowsBySnapshotId(
      List<StatisticsFile> statisticsFiles, Schema tableSchema) {
    Map<Long, List<StaticDataTask.Row>> rowsBySnapshotId = Maps.newHashMap();

    for (StatisticsFile statisticsFile : statisticsFiles) {
      long referenceSnapshotId = statisticsFile.snapshotId();
      StaticDataTask.Row row =
          puffinFileToRow(tableSchema, PuffinFileRow.fromStatisticsFile(statisticsFile));

      rowsBySnapshotId
          .computeIfAbsent(referenceSnapshotId, ignored -> Lists.newArrayList())
          .add(row);
    }

    return rowsBySnapshotId;
  }

  private static boolean mayHaveDeleteFiles(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    if (summary == null) {
      return true;
    }

    String totalDeleteFiles = summary.get(SnapshotSummary.TOTAL_DELETE_FILES_PROP);
    try {
      return totalDeleteFiles == null || Long.parseLong(totalDeleteFiles) > 0L;
    } catch (NumberFormatException ignored) {
      return true;
    }
  }

  private static List<PuffinFileRow> scanDeletionVectorPuffinFiles(
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      long referenceSnapshotId,
      ManifestListFile manifestList) {
    Map<String, DeletionVectorFileAccumulator> dvFiles = Maps.newLinkedHashMap();

    try (CloseableIterable<ManifestFile> manifests = readManifestList(io, manifestList)) {
      for (ManifestFile manifest : manifests) {
        if (manifest.content() == ManifestContent.DELETES) {
          accumulateDeletionVectorsFromManifest(
              io, specsById, referenceSnapshotId, manifest, dvFiles);
        }
      }

    } catch (IOException e) {
      throw new RuntimeIOException(
          e,
          "Failed to read manifest list for snapshot %s: %s",
          referenceSnapshotId,
          manifestList.location());
    }

    return dvFiles.values().stream()
        .map(DeletionVectorFileAccumulator::toPuffinFileRow)
        .collect(ImmutableList.toImmutableList());
  }

  private static CloseableIterable<ManifestFile> readManifestList(
      FileIO io, ManifestListFile manifestList) {
    return InternalData.read(FileFormat.AVRO, io.newInputFile(manifestList))
        .setRootType(GenericManifestFile.class)
        .setCustomType(
            ManifestFile.PARTITION_SUMMARIES_ELEMENT_ID, GenericPartitionFieldSummary.class)
        .project(ManifestFile.schema())
        .build();
  }

  private static void accumulateDeletionVectorsFromManifest(
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      long referenceSnapshotId,
      ManifestFile deleteManifest,
      Map<String, DeletionVectorFileAccumulator> dvFiles) {
    try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(deleteManifest, io, specsById)
                .select(DV_SCAN_COLUMNS);
        CloseableIterable<ManifestEntry<DeleteFile>> entries = reader.liveEntries()) {

      for (ManifestEntry<DeleteFile> entry : entries) {
        DeleteFile deleteFile = entry.file();
        if (!ContentFileUtil.isDV(deleteFile)) {
          continue;
        }

        String path = deleteFile.location();
        Long contentOffset = deleteFile.contentOffset();
        Long contentSizeInBytes = deleteFile.contentSizeInBytes();

        Preconditions.checkState(
            contentOffset != null,
            "Missing content offset for deletion vector: snapshot=%s, manifest=%s, path=%s",
            referenceSnapshotId,
            deleteManifest.path(),
            path);
        Preconditions.checkState(
            contentSizeInBytes != null,
            "Missing content size for deletion vector: snapshot=%s, manifest=%s, path=%s",
            referenceSnapshotId,
            deleteManifest.path(),
            path);

        long fileSizeInBytes = deleteFile.fileSizeInBytes();

        dvFiles
            .computeIfAbsent(
                path,
                ignored ->
                    new DeletionVectorFileAccumulator(referenceSnapshotId, path, fileSizeInBytes))
            .add(fileSizeInBytes, contentOffset, contentSizeInBytes);
      }

    } catch (IOException e) {
      throw new RuntimeIOException(
          e,
          "Failed to read delete manifest for snapshot %s: %s",
          referenceSnapshotId,
          deleteManifest.path());
    }
  }

  private static StaticDataTask.Row puffinFileToRow(Schema tableSchema, PuffinFileRow puffinFile) {
    ImmutableList.Builder<StaticDataTask.Row> referencedFields =
        ImmutableList.builderWithExpectedSize(puffinFile.referencedFieldIds().size());

    for (Integer fieldId : puffinFile.referencedFieldIds()) {
      referencedFields.add(StaticDataTask.Row.of(fieldId, currentFieldName(tableSchema, fieldId)));
    }

    return StaticDataTask.Row.of(
        puffinFile.referenceSnapshotId(),
        puffinFile.path(),
        puffinFile.source(),
        puffinFile.fileSizeInBytes(),
        puffinFile.referencedBlobCount(),
        puffinFile.referencedBlobTypes(),
        referencedFields.build());
  }

  private static String currentFieldName(Schema tableSchema, int fieldId) {
    if (fieldId == MetadataColumns.ROW_POSITION.fieldId()) {
      return MetadataColumns.ROW_POSITION.name();
    }

    return tableSchema.findColumnName(fieldId);
  }

  private static long estimatedDvTaskSize(Snapshot snapshot) {
    long deleteFileCount = totalDeleteFileCount(snapshot);

    if (deleteFileCount <= 1_000L) {
      return 4L * 1024 * 1024;
    } else if (deleteFileCount <= 10_000L) {
      return 8L * 1024 * 1024;
    } else if (deleteFileCount <= 50_000L) {
      return 16L * 1024 * 1024;
    } else {
      return 32L * 1024 * 1024;
    }
  }

  private static long totalDeleteFileCount(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    if (summary == null) {
      return 1L;
    }

    String value = summary.get(SnapshotSummary.TOTAL_DELETE_FILES_PROP);
    if (value == null) {
      return 1L;
    }

    try {
      return Math.max(1L, Long.parseLong(value));
    } catch (NumberFormatException ignored) {
      return 1L;
    }
  }

  private static class AllPuffinFilesTableScan extends BaseAllMetadataTableScan {

    AllPuffinFilesTableScan(Table table, Schema schema) {
      super(table, schema, MetadataTableType.ALL_PUFFIN_FILES);
    }

    private AllPuffinFilesTableScan(Table table, Schema schema, TableScanContext context) {
      super(table, schema, MetadataTableType.ALL_PUFFIN_FILES, context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new AllPuffinFilesTableScan(table, schema, context);
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      BaseTable baseTable = (BaseTable) table();
      TableMetadata metadata = baseTable.operations().current();
      Schema tableSchema = metadata.schema();
      Schema projectedSchema = schema();

      Expression planningFilter = filter();
      Expression residual = residualFilter();
      SnapshotReferenceEvaluator taskEvaluator =
          new SnapshotReferenceEvaluator(
              planningFilter,
              ALL_PUFFIN_FILES_SCHEMA.asStruct(),
              REFERENCE_SNAPSHOT_ID.fieldId(),
              SOURCE.fieldId(),
              isCaseSensitive());

      boolean mayScanStatistics = taskEvaluator.mayMatchSource(SOURCE_STATISTICS);
      boolean mayScanDeletionVectors =
          metadata.formatVersion() >= MIN_FORMAT_VERSION_DELETION_VECTORS
              && taskEvaluator.mayMatchSource(SOURCE_DELETION_VECTOR);

      Map<Long, List<StaticDataTask.Row>> statisticsRowsBySnapshotId =
          mayScanStatistics
              ? AllPuffinFilesTable.statisticsRowsBySnapshotId(
                  metadata.statisticsFiles(), tableSchema)
              : ImmutableMap.of();
      Map<Integer, PartitionSpec> specsById =
          mayScanDeletionVectors ? ImmutableMap.copyOf(metadata.specsById()) : ImmutableMap.of();

      String location =
          metadata.metadataFileLocation() != null
              ? metadata.metadataFileLocation()
              : table().location();
      ImmutableList.Builder<FileScanTask> tasks = ImmutableList.builder();

      for (Snapshot snapshot : metadata.snapshots()) {
        addStatisticsTask(
            tasks,
            baseTable,
            projectedSchema,
            location,
            snapshot,
            mayScanStatistics,
            taskEvaluator,
            statisticsRowsBySnapshotId);

        addDeletionVectorTask(
            tasks,
            baseTable,
            tableSchema,
            projectedSchema,
            specsById,
            residual,
            snapshot,
            mayScanDeletionVectors,
            taskEvaluator);
      }
      return CloseableIterable.withNoopClose(tasks.build());
    }

    private static void addStatisticsTask(
        ImmutableList.Builder<FileScanTask> tasks,
        BaseTable baseTable,
        Schema projectedSchema,
        String location,
        Snapshot snapshot,
        boolean mayScanStatistics,
        SnapshotReferenceEvaluator taskEvaluator,
        Map<Long, List<StaticDataTask.Row>> statisticsRowsBySnapshotId) {

      if (!mayScanStatistics || !taskEvaluator.mayMatch(snapshot, SOURCE_STATISTICS)) {
        return;
      }

      List<StaticDataTask.Row> statisticsRows =
          statisticsRowsBySnapshotId.getOrDefault(snapshot.snapshotId(), ImmutableList.of());

      if (!statisticsRows.isEmpty()) {
        tasks.add(
            StaticDataTask.of(
                baseTable.io().newInputFile(location),
                ALL_PUFFIN_FILES_SCHEMA,
                projectedSchema,
                statisticsRows,
                row -> row));
      }
    }

    private static void addDeletionVectorTask(
        ImmutableList.Builder<FileScanTask> tasks,
        BaseTable baseTable,
        Schema tableSchema,
        Schema projectedSchema,
        Map<Integer, PartitionSpec> specsById,
        Expression residual,
        Snapshot snapshot,
        boolean mayScanDeletionVectors,
        SnapshotReferenceEvaluator taskEvaluator) {

      String manifestListLocation = snapshot.manifestListLocation();
      boolean shouldScanDeletionVectors =
          mayScanDeletionVectors
              && manifestListLocation != null
              && mayHaveDeleteFiles(snapshot)
              && taskEvaluator.mayMatch(snapshot, SOURCE_DELETION_VECTOR);

      if (!shouldScanDeletionVectors) {
        return;
      }

      tasks.add(
          new DeletionVectorPuffinFilesTask(
              baseTable.io(),
              tableSchema,
              projectedSchema,
              specsById,
              residual,
              snapshot.snapshotId(),
              new BaseManifestListFile(manifestListLocation, snapshot.keyId()),
              estimatedDvTaskSize(snapshot)));
    }
  }

  /**
   * Conservatively evaluates a metadata-table filter using the snapshot ID and Puffin metadata
   * source values that are known during task planning.
   *
   * <p>The evaluator makes exact decisions only for predicates over supported fields whose values
   * are known in the current evaluation context. Predicates over unknown values or unsupported
   * fields return {@link MatchResult#MIGHT_MATCH}. The three-state result is important for NOT:
   * negating an unknown predicate is still unknown and must not prune a task.
   */
  private static class SnapshotReferenceEvaluator {

    private final int referenceSnapshotIdFieldId;
    private final int sourceFieldId;
    private final Expression boundExpression;

    private SnapshotReferenceEvaluator(
        Expression expression,
        Types.StructType structType,
        int referenceSnapshotIdFieldId,
        int sourceFieldId,
        boolean caseSensitive) {
      this.referenceSnapshotIdFieldId = referenceSnapshotIdFieldId;
      this.sourceFieldId = sourceFieldId;
      this.boundExpression = Binder.bind(structType, expression, caseSensitive);
    }

    private boolean mayMatch(Snapshot snapshot, String source) {
      return new SnapshotEvalVisitor(snapshot.snapshotId(), source).eval()
          != MatchResult.CANNOT_MATCH;
    }

    private boolean mayMatchSource(String source) {
      return new SnapshotEvalVisitor(source).eval() != MatchResult.CANNOT_MATCH;
    }

    private enum MatchResult {
      CANNOT_MATCH,
      MIGHT_MATCH,
      MUST_MATCH;

      private static MatchResult from(boolean matches) {
        return matches ? MUST_MATCH : CANNOT_MATCH;
      }

      private MatchResult negate() {
        return switch (this) {
          case CANNOT_MATCH -> MUST_MATCH;
          case MUST_MATCH -> CANNOT_MATCH;
          case MIGHT_MATCH -> MIGHT_MATCH;
        };
      }

      private static MatchResult and(MatchResult left, MatchResult right) {
        if (left == CANNOT_MATCH || right == CANNOT_MATCH) {
          return CANNOT_MATCH;
        } else if (left == MUST_MATCH && right == MUST_MATCH) {
          return MUST_MATCH;
        }

        return MIGHT_MATCH;
      }

      private static MatchResult or(MatchResult left, MatchResult right) {
        if (left == MUST_MATCH || right == MUST_MATCH) {
          return MUST_MATCH;
        } else if (left == CANNOT_MATCH && right == CANNOT_MATCH) {
          return CANNOT_MATCH;
        }

        return MIGHT_MATCH;
      }
    }

    private class SnapshotEvalVisitor extends BoundExpressionVisitor<MatchResult> {

      private final boolean snapshotIdKnown;
      private final long snapshotId;
      private final String source;

      private SnapshotEvalVisitor(long snapshotId, String source) {
        this.snapshotIdKnown = true;
        this.snapshotId = snapshotId;
        this.source = source;
      }

      private SnapshotEvalVisitor(String source) {
        this.snapshotIdKnown = false;
        this.snapshotId = 0L;
        this.source = source;
      }

      private MatchResult eval() {
        return ExpressionVisitors.visit(boundExpression, this);
      }

      @Override
      public MatchResult alwaysTrue() {
        return MatchResult.MUST_MATCH;
      }

      @Override
      public MatchResult alwaysFalse() {
        return MatchResult.CANNOT_MATCH;
      }

      @Override
      public MatchResult not(MatchResult result) {
        return result.negate();
      }

      @Override
      public MatchResult and(MatchResult leftResult, MatchResult rightResult) {
        return MatchResult.and(leftResult, rightResult);
      }

      @Override
      public MatchResult or(MatchResult leftResult, MatchResult rightResult) {
        return MatchResult.or(leftResult, rightResult);
      }

      @Override
      public <T> MatchResult isNull(BoundReference<T> reference) {
        return isSupportedReference(reference) ? MatchResult.CANNOT_MATCH : MatchResult.MIGHT_MATCH;
      }

      @Override
      public <T> MatchResult notNull(BoundReference<T> reference) {
        return isSupportedReference(reference) ? MatchResult.MUST_MATCH : MatchResult.MIGHT_MATCH;
      }

      @Override
      public <T> MatchResult isNaN(BoundReference<T> reference) {
        return MatchResult.MIGHT_MATCH;
      }

      @Override
      public <T> MatchResult notNaN(BoundReference<T> reference) {
        return MatchResult.MIGHT_MATCH;
      }

      @Override
      public <T> MatchResult lt(BoundReference<T> reference, Literal<T> literal) {
        return evaluateComparison(reference, literal, compareResult -> compareResult < 0);
      }

      @Override
      public <T> MatchResult ltEq(BoundReference<T> reference, Literal<T> literal) {
        return evaluateComparison(reference, literal, compareResult -> compareResult <= 0);
      }

      @Override
      public <T> MatchResult gt(BoundReference<T> reference, Literal<T> literal) {
        return evaluateComparison(reference, literal, compareResult -> compareResult > 0);
      }

      @Override
      public <T> MatchResult gtEq(BoundReference<T> reference, Literal<T> literal) {
        return evaluateComparison(reference, literal, compareResult -> compareResult >= 0);
      }

      @Override
      public <T> MatchResult eq(BoundReference<T> reference, Literal<T> literal) {
        return evaluateComparison(reference, literal, compareResult -> compareResult == 0);
      }

      @Override
      public <T> MatchResult notEq(BoundReference<T> reference, Literal<T> literal) {
        return eq(reference, literal).negate();
      }

      @Override
      public <T> MatchResult in(BoundReference<T> reference, Set<T> literalSet) {
        if (isReferenceSnapshotId(reference)) {
          if (!snapshotIdKnown) {
            return MatchResult.MIGHT_MATCH;
          }

          return MatchResult.from(literalSet.contains(snapshotId));
        } else if (isSource(reference)) {
          return MatchResult.from(literalSet.contains(source));
        }

        return MatchResult.MIGHT_MATCH;
      }

      @Override
      public <T> MatchResult notIn(BoundReference<T> reference, Set<T> literalSet) {
        return in(reference, literalSet).negate();
      }

      @Override
      public <T> MatchResult startsWith(BoundReference<T> reference, Literal<T> literal) {
        if (!isSource(reference)) {
          return MatchResult.MIGHT_MATCH;
        }

        Literal<CharSequence> stringLiteral = literal.to(Types.StringType.get());
        Preconditions.checkState(
            stringLiteral != null, "Cannot convert literal to string for source: %s", literal);
        return MatchResult.from(source.startsWith(stringLiteral.value().toString()));
      }

      @Override
      public <T> MatchResult notStartsWith(BoundReference<T> reference, Literal<T> literal) {
        return startsWith(reference, literal).negate();
      }

      @Override
      public <T> MatchResult handleNonReference(Bound<T> term) {
        return MatchResult.MIGHT_MATCH;
      }

      private <T> MatchResult evaluateComparison(
          BoundReference<T> reference, Literal<T> literal, IntPredicate matches) {
        int compareResult;
        if (isReferenceSnapshotId(reference)) {
          if (!snapshotIdKnown) {
            return MatchResult.MIGHT_MATCH;
          }

          Literal<Long> longLiteral = literal.to(Types.LongType.get());
          Preconditions.checkState(
              longLiteral != null,
              "Cannot convert literal to long for reference_snapshot_id: %s",
              literal);
          compareResult = longLiteral.comparator().compare(snapshotId, longLiteral.value());

        } else if (isSource(reference)) {
          Literal<CharSequence> stringLiteral = literal.to(Types.StringType.get());
          Preconditions.checkState(
              stringLiteral != null, "Cannot convert literal to string for source: %s", literal);
          compareResult = stringLiteral.comparator().compare(source, stringLiteral.value());

        } else {
          return MatchResult.MIGHT_MATCH;
        }

        return MatchResult.from(matches.test(compareResult));
      }

      private boolean isSupportedReference(BoundReference<?> reference) {
        return isReferenceSnapshotId(reference) || isSource(reference);
      }

      private boolean isReferenceSnapshotId(BoundReference<?> reference) {
        return reference.fieldId() == referenceSnapshotIdFieldId;
      }

      private boolean isSource(BoundReference<?> reference) {
        return reference.fieldId() == sourceFieldId;
      }
    }
  }

  private static class DeletionVectorPuffinFilesTask implements DataTask {

    private final FileIO io;
    private final Schema tableSchema;
    private final Schema projectedSchema;
    private final Map<Integer, PartitionSpec> specsById;
    private final Expression residual;
    private final long referenceSnapshotId;
    private final ManifestListFile manifestList;
    private final long estimatedTaskSize;

    private DataFile lazyDataFile = null;

    private DeletionVectorPuffinFilesTask(
        FileIO io,
        Schema tableSchema,
        Schema projectedSchema,
        Map<Integer, PartitionSpec> specsById,
        Expression residual,
        long referenceSnapshotId,
        ManifestListFile manifestList,
        long estimatedTaskSize) {
      this.io = io;
      this.tableSchema = tableSchema;
      this.projectedSchema = projectedSchema;
      this.specsById = specsById;
      this.residual = residual;
      this.referenceSnapshotId = referenceSnapshotId;
      this.manifestList = manifestList;
      this.estimatedTaskSize = estimatedTaskSize;
    }

    @Override
    public List<DeleteFile> deletes() {
      return ImmutableList.of();
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      List<PuffinFileRow> puffinFiles =
          scanDeletionVectorPuffinFiles(io, specsById, referenceSnapshotId, manifestList);

      CloseableIterable<StructLike> rows =
          CloseableIterable.transform(
              CloseableIterable.withNoopClose(puffinFiles),
              puffinFile -> puffinFileToRow(tableSchema, puffinFile));

      StructProjection projection =
          StructProjection.create(ALL_PUFFIN_FILES_SCHEMA, projectedSchema);

      return CloseableIterable.transform(rows, projection::wrap);
    }

    @Override
    public DataFile file() {
      if (lazyDataFile == null) {
        // The actual result row count is unknown until the manifests are scanned.
        this.lazyDataFile =
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(io.newInputFile(manifestList))
                .withFormat(FileFormat.AVRO)
                .withRecordCount(1L)
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
      return 0L;
    }

    @Override
    public long length() {
      return estimatedTaskSize;
    }

    @Override
    public Expression residual() {
      return residual;
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this);
    }

    @Override
    public Schema schema() {
      return projectedSchema;
    }
  }

  private record PuffinFileRow(
      long referenceSnapshotId,
      String path,
      String source,
      long fileSizeInBytes,
      int referencedBlobCount,
      List<String> referencedBlobTypes,
      List<Integer> referencedFieldIds) {

    private static PuffinFileRow fromStatisticsFile(StatisticsFile statisticsFile) {
      List<BlobMetadata> blobs = statisticsFile.blobMetadata();

      List<String> referencedBlobTypes =
          blobs.stream()
              .map(BlobMetadata::type)
              .distinct()
              .collect(ImmutableList.toImmutableList());

      List<Integer> referencedFieldIds =
          blobs.stream()
              .flatMap(blob -> blob.fields().stream())
              .distinct()
              .collect(ImmutableList.toImmutableList());

      return new PuffinFileRow(
          statisticsFile.snapshotId(),
          statisticsFile.path(),
          SOURCE_STATISTICS,
          statisticsFile.fileSizeInBytes(),
          blobs.size(),
          referencedBlobTypes,
          referencedFieldIds);
    }
  }

  private static class DeletionVectorFileAccumulator {

    private final long referenceSnapshotId;
    private final String path;
    private final long fileSizeInBytes;
    private final Set<DeletionVectorBlobKey> referencedBlobs = Sets.newLinkedHashSet();

    private DeletionVectorFileAccumulator(
        long referenceSnapshotId, String path, long fileSizeInBytes) {
      this.referenceSnapshotId = referenceSnapshotId;
      this.path = path;
      this.fileSizeInBytes = fileSizeInBytes;
    }

    private void add(long entryFileSizeInBytes, long contentOffset, long contentSizeInBytes) {
      Preconditions.checkState(
          fileSizeInBytes == entryFileSizeInBytes,
          "Deletion vectors in the same Puffin file have different file sizes: "
              + "path=%s, referenceSnapshotId=%s, expected=%s, actual=%s",
          path,
          referenceSnapshotId,
          fileSizeInBytes,
          entryFileSizeInBytes);
      referencedBlobs.add(new DeletionVectorBlobKey(contentOffset, contentSizeInBytes));
    }

    private PuffinFileRow toPuffinFileRow() {
      return new PuffinFileRow(
          referenceSnapshotId,
          path,
          SOURCE_DELETION_VECTOR,
          fileSizeInBytes,
          referencedBlobs.size(),
          DV_REFERENCED_BLOB_TYPES,
          DV_REFERENCED_FIELD_IDS);
    }
  }

  private record DeletionVectorBlobKey(long contentOffset, long contentSizeInBytes) {}
}
