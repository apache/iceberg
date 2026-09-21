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
package org.apache.iceberg.spark.procedures;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.index.HashTransform;
import org.apache.iceberg.index.IndexCatalog;
import org.apache.iceberg.index.IndexIdentifier;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexSnapshot;
import org.apache.iceberg.index.LeafFileEntry;
import org.apache.iceberg.index.LeafFileMetadata;
import org.apache.iceberg.index.LeafFileWriter;
import org.apache.iceberg.index.ScalarIndexCommitter;
import org.apache.iceberg.index.TrackingFileEntry;
import org.apache.iceberg.index.TrackingFileReader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.IndexSnapshotUtil;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkIndexCatalogs;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A procedure that builds a SCALAR index on a single key column.
 *
 * <p>Builds the index by reading the source table, computing each row's position within its
 * source file (before any shuffle, so file-scan order is preserved), computing the transform
 * value (HASH bucket, or the key value itself for IDENTITY), sorting by {@code (transform_value,
 * key_value)}, and writing one leaf file per resulting Spark partition. Commits the result through
 * {@link ScalarIndexCommitter} into a session-scoped {@link IndexCatalog} ({@link
 * SparkIndexCatalogs}) -- see that class's javadoc for the persistence limitation this implies.
 *
 * <p>Only a single key column is supported, matching the current SCALAR proposal's scope
 * (multi-column composite indexes are an explicit Non-Goal). {@code IDENTITY} additionally
 * requires a numeric (long or int) key column, since the transform value is a {@code long} and a
 * string cannot be cast to one meaningfully.
 *
 * <p>Defaults to a full rebuild -- reading the entire source table and rewriting every leaf file
 * -- every time it is called. Passing {@code options => map('mode', 'incremental')} builds only
 * the leaf files for data files added since the existing index's last snapshot, appending them to
 * the existing leaf files rather than rewriting everything (the append-only option from Huaxin
 * Gao's Primary Key Index for Apache Iceberg proposal, Section 7.2). Falls back to a full rebuild
 * if there is no existing index to build on incrementally, or if anything about determining the
 * added files fails (for example, a compaction/rewrite happened since the index was last built --
 * see {@link org.apache.iceberg.spark.IndexSnapshotUtil#addedFilePathsSince}).
 */
class BuildScalarIndexProcedure extends BaseProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(BuildScalarIndexProcedure.class);

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMNS_PARAM =
      requiredInParameter("columns", STRING_ARRAY);
  private static final ProcedureParameter TRANSFORM_PARAM =
      requiredInParameter("transform", DataTypes.StringType);
  private static final ProcedureParameter OPTIONS_PARAM =
      optionalInParameter("options", STRING_MAP);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMNS_PARAM, TRANSFORM_PARAM, OPTIONS_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("index_location", DataTypes.StringType, false, Metadata.empty()),
            new StructField("leaf_file_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("record_count", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<BuildScalarIndexProcedure>() {
      @Override
      protected BuildScalarIndexProcedure doBuild() {
        return new BuildScalarIndexProcedure(tableCatalog());
      }
    };
  }

  private BuildScalarIndexProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    Identifier tableIdent = input.ident(TABLE_PARAM);
    String[] columns = input.asStringArray(COLUMNS_PARAM);
    String transformName = input.asString(TRANSFORM_PARAM);
    Map<String, String> options = input.asStringMap(OPTIONS_PARAM, ImmutableMap.of());

    Preconditions.checkArgument(
        columns != null && columns.length == 1,
        "build_scalar_index supports exactly one key column (multi-column composite indexes are"
            + " not yet supported), got: %s",
        columns == null ? 0 : columns.length);
    String keyColumnName = columns[0];

    return withIcebergTable(
        tableIdent,
        table -> {
          BuildResult result = buildAndCommit(tableIdent, table, keyColumnName, transformName, options);
          return toOutputRows(result);
        });
  }

  private BuildResult buildAndCommit(
      Identifier tableIdent,
      Table table,
      String keyColumnName,
      String transformName,
      Map<String, String> options) {
    Schema schema = table.schema();
    Types.NestedField keyField = schema.findField(keyColumnName);
    Preconditions.checkArgument(
        keyField != null, "Column '%s' does not exist in table schema", keyColumnName);

    String upperTransform = transformName.toUpperCase(Locale.ROOT);
    Column transformValueCol = transformValueColumn(upperTransform, keyField, options);

    // Derived from the core Table's own name, not the Spark catalog Identifier -- must match
    // exactly how SparkScanBuilder derives it on the read side, or indexExists() there silently
    // and permanently returns false.
    TableIdentifier icebergTableIdent = TableIdentifier.parse(table.name());
    IndexIdentifier indexIdent = IndexIdentifier.of(icebergTableIdent, keyColumnName + "_idx");
    IndexCatalog catalog = SparkIndexCatalogs.get().catalogFor(table);

    String mode = options.getOrDefault("mode", "full").toLowerCase(Locale.ROOT);
    if ("incremental".equals(mode) && catalog.indexExists(indexIdent)) {
      try {
        return buildIncremental(
            catalog, indexIdent, tableIdent, table, keyField, upperTransform, transformValueCol,
            keyColumnName, options);
      } catch (Exception e) {
        LOG.warn(
            "Incremental build failed for index {}, falling back to full rebuild: {}",
            indexIdent,
            e.getMessage());
      }
    }

    return buildFull(
        catalog, indexIdent, tableIdent, table, keyField, upperTransform, transformValueCol,
        keyColumnName, options);
  }

  private BuildResult buildFull(
      IndexCatalog catalog,
      IndexIdentifier indexIdent,
      Identifier tableIdent,
      Table table,
      Types.NestedField keyField,
      String upperTransform,
      Column transformValueCol,
      String keyColumnName,
      Map<String, String> options) {
    String tableName = Spark3Util.quotedFullIdentifier(tableCatalog().name(), tableIdent);
    Dataset<Row> sourceDf = spark().read().table(tableName);

    int targetLeafFiles = Integer.parseInt(options.getOrDefault("target-leaf-files", "4"));
    String indexLocation =
        options.getOrDefault(
            "location", stripTrailingSlash(table.location()) + "/index/" + keyColumnName + "_idx");
    String leafDataLocation = indexLocation + "/data";
    FileIO io = table.io();

    List<LeafFileWriteResult> writeResults =
        buildLeafFiles(sourceDf, keyColumnName, transformValueCol, keyField, io, leafDataLocation, targetLeafFiles);
    List<LeafFileMetadata> leafFiles = toLeafFileMetadata(writeResults);

    Preconditions.checkArgument(
        !leafFiles.isEmpty(), "build_scalar_index produced no leaf files -- source table is empty?");

    ScalarIndexCommitter committer = new ScalarIndexCommitter(catalog, io);
    committer.commit(
        indexIdent,
        table.uuid().toString(),
        table.currentSnapshot().snapshotId(),
        "SCALAR",
        upperTransform,
        ImmutableList.of(keyField.fieldId()),
        indexLocation,
        leafFiles);

    return new BuildResult(indexLocation, leafFiles.size(), sumRecordCount(leafFiles));
  }

  /**
   * Builds only the leaf files for data files added to the table since the existing index's last
   * snapshot, and appends them to the existing leaf files rather than rewriting everything -- the
   * append-only option from Huaxin Gao's Primary Key Index for Apache Iceberg proposal (Section
   * 7.2). Stale entries (e.g. from rows since updated or deleted) are not removed here; a full
   * rebuild is what cleans those up, matching that same proposal's recommendation.
   *
   * <p>Throws (letting {@link #buildAndCommit} fall back to a full rebuild) if the existing
   * index's key column doesn't match, has no committed snapshot, or if {@link
   * IndexSnapshotUtil#addedFilePathsSince} can't safely determine the added files (for example, a
   * compaction/rewrite happened since the index was last built).
   */
  private BuildResult buildIncremental(
      IndexCatalog catalog,
      IndexIdentifier indexIdent,
      Identifier tableIdent,
      Table table,
      Types.NestedField keyField,
      String upperTransform,
      Column transformValueCol,
      String keyColumnName,
      Map<String, String> options) {
    Preconditions.checkArgument(
        table.currentSnapshot() != null, "Cannot incrementally build an index on an empty table");

    IndexMetadata existing = catalog.loadIndex(indexIdent);
    Preconditions.checkArgument(
        existing.keyColumnIds().equals(ImmutableList.of(keyField.fieldId())),
        "Existing index %s is on a different key column; incremental build requires the same"
            + " key column",
        indexIdent);
    IndexSnapshot existingSnapshot = existing.currentSnapshot();
    Preconditions.checkArgument(
        existingSnapshot != null,
        "Existing index %s has no committed snapshot to build on incrementally",
        indexIdent);

    long currentTableSnapshotId = table.currentSnapshot().snapshotId();
    long sourceTableSnapshotId = existingSnapshot.sourceTableSnapshotId();
    FileIO io = table.io();

    List<LeafFileMetadata> existingLeafFiles =
        TrackingFileReader.readAll(io.newInputFile(existingSnapshot.trackingFile())).stream()
            .map(BuildScalarIndexProcedure::toLeafFileMetadata)
            .collect(Collectors.toList());

    ScalarIndexCommitter committer = new ScalarIndexCommitter(catalog, io);

    if (sourceTableSnapshotId == currentTableSnapshotId) {
      // Already fresh -- nothing new to index. Commit is a no-op in effect (same leaf files,
      // same source snapshot), so just report the existing state back rather than churn a new
      // index snapshot for no reason.
      return new BuildResult(
          existing.location(), existingLeafFiles.size(), sumRecordCount(existingLeafFiles));
    }

    Set<String> addedFilePaths =
        IndexSnapshotUtil.addedFilePathsSince(table, sourceTableSnapshotId, currentTableSnapshotId);

    if (addedFilePaths.isEmpty()) {
      // No new data files since the index was last built -- just refresh the snapshot pointer,
      // no new leaf files needed.
      committer.commit(
          indexIdent,
          table.uuid().toString(),
          currentTableSnapshotId,
          "SCALAR",
          upperTransform,
          ImmutableList.of(keyField.fieldId()),
          existing.location(),
          existingLeafFiles);
      return new BuildResult(
          existing.location(), existingLeafFiles.size(), sumRecordCount(existingLeafFiles));
    }

    String tableName = Spark3Util.quotedFullIdentifier(tableCatalog().name(), tableIdent);
    Dataset<Row> newRowsDf =
        spark().read().table(tableName).filter(input_file_name().isInCollection(addedFilePaths));

    int targetLeafFiles = Integer.parseInt(options.getOrDefault("target-leaf-files", "4"));
    String leafDataLocation = existing.location() + "/data";

    List<LeafFileWriteResult> newWriteResults =
        buildLeafFiles(newRowsDf, keyColumnName, transformValueCol, keyField, io, leafDataLocation, targetLeafFiles);
    List<LeafFileMetadata> newLeafFiles = toLeafFileMetadata(newWriteResults);
    Preconditions.checkArgument(
        !newLeafFiles.isEmpty(),
        "Incremental build found added files but produced no leaf files -- this should not happen");

    List<LeafFileMetadata> allLeafFiles = Lists.newArrayList(existingLeafFiles);
    allLeafFiles.addAll(newLeafFiles);

    committer.commit(
        indexIdent,
        table.uuid().toString(),
        currentTableSnapshotId,
        "SCALAR",
        upperTransform,
        ImmutableList.of(keyField.fieldId()),
        existing.location(),
        allLeafFiles);

    return new BuildResult(existing.location(), allLeafFiles.size(), sumRecordCount(allLeafFiles));
  }

  /**
   * Computes position/transform-value/leaf-file assignment and writes leaf files for {@code
   * sourceDf}'s rows, shared between {@link #buildFull} (the whole source table) and {@link
   * #buildIncremental} (only newly added rows).
   */
  private List<LeafFileWriteResult> buildLeafFiles(
      Dataset<Row> sourceDf,
      String keyColumnName,
      Column transformValueCol,
      Types.NestedField keyField,
      FileIO io,
      String leafDataLocation,
      int targetLeafFiles) {
    // Compute position before any shuffle, so it reflects physical file-scan order.
    // row_number() returns IntegerType, not LongType -- cast explicitly so __position is
    // genuinely a long column, matching LeafFileEntry.position()'s type. Reading an
    // Integer-backed column via row.getAs() with an inferred Long type throws
    // ClassCastException at runtime, not a compile error, so this must be fixed at the source.
    Dataset<Row> withPosition =
        sourceDf
            .select(
                col(keyColumnName).as("__key"), input_file_name().as("__source_file_path"))
            .withColumn(
                "__position",
                row_number()
                    .over(
                        Window.partitionBy("__source_file_path")
                            .orderBy(monotonically_increasing_id()))
                    .minus(1)
                    .cast(DataTypes.LongType));

    Dataset<Row> withTransform = withPosition.withColumn("__transform_value", transformValueCol);

    Dataset<Row> sorted =
        withTransform
            .repartitionByRange(targetLeafFiles, col("__transform_value"))
            .sortWithinPartitions(col("__transform_value"), col("__key"));

    return sorted
        .mapPartitions(
            (MapPartitionsFunction<Row, LeafFileWriteResult>)
                rows -> writeLeafFilePartition(rows, io, keyField, leafDataLocation),
            Encoders.javaSerialization(LeafFileWriteResult.class))
        .collectAsList();
  }

  private static List<LeafFileMetadata> toLeafFileMetadata(List<LeafFileWriteResult> writeResults) {
    List<LeafFileMetadata> leafFiles = Lists.newArrayList();
    for (LeafFileWriteResult r : writeResults) {
      leafFiles.add(
          new LeafFileMetadata(
              r.path, "parquet", r.recordCount, r.sizeBytes, r.transformValueMin,
              r.transformValueMax));
    }
    return leafFiles;
  }

  private static LeafFileMetadata toLeafFileMetadata(TrackingFileEntry entry) {
    return new LeafFileMetadata(
        entry.location(),
        entry.fileFormat(),
        entry.recordCount(),
        entry.fileSizeInBytes(),
        entry.transformValueLowerBound(),
        entry.transformValueUpperBound());
  }

  private static long sumRecordCount(List<LeafFileMetadata> leafFiles) {
    long total = 0;
    for (LeafFileMetadata leafFile : leafFiles) {
      total += leafFile.recordCount();
    }
    return total;
  }

  private Column transformValueColumn(
      String upperTransform, Types.NestedField keyField, Map<String, String> options) {
    Type keyType = keyField.type();
    // References "__key", the alias buildAndCommit's earlier .select() gives the key column --
    // by the time this runs, the original column name no longer exists in the DataFrame's schema.

    if ("HASH".equals(upperTransform)) {
      int numBuckets = Integer.parseInt(options.getOrDefault("hash.num-buckets", "256"));
      HashTransform transform = new HashTransform(numBuckets);
      UserDefinedFunction udf;
      switch (keyType.typeId()) {
        case STRING:
          udf = functions.udf((UDF1<String, Long>) transform::apply, DataTypes.LongType);
          break;
        case LONG:
          udf =
              functions.udf(
                  (UDF1<Long, Long>) v -> transform.apply(v.longValue()), DataTypes.LongType);
          break;
        case INTEGER:
          udf =
              functions.udf(
                  (UDF1<Integer, Long>) v -> transform.apply(v.intValue()), DataTypes.LongType);
          break;
        default:
          throw new IllegalArgumentException(
              "HASH transform does not support key column type: " + keyType);
      }
      return udf.apply(col("__key"));
    } else if ("IDENTITY".equals(upperTransform)) {
      Preconditions.checkArgument(
          keyType.typeId() == Type.TypeID.LONG || keyType.typeId() == Type.TypeID.INTEGER,
          "IDENTITY transform requires a numeric (long or int) key column, got: %s",
          keyType);
      return col("__key").cast(DataTypes.LongType);
    } else {
      throw new IllegalArgumentException(
          "Unsupported transform '" + upperTransform + "': expected HASH or IDENTITY");
    }
  }

  private static Iterator<LeafFileWriteResult> writeLeafFilePartition(
      Iterator<Row> rows, FileIO io, Types.NestedField keyField, String leafDataLocation) {
    if (!rows.hasNext()) {
      return java.util.Collections.emptyIterator();
    }

    String path = leafDataLocation + "/leaf-" + UUID.randomUUID() + ".parquet";
    long recordCount = 0;
    long tvMin = Long.MAX_VALUE;
    long tvMax = Long.MIN_VALUE;

    try (LeafFileWriter writer = new LeafFileWriter(io.newOutputFile(path), keyField)) {
      while (rows.hasNext()) {
        Row row = rows.next();
        Object keyValue = row.getAs("__key");
        long transformValue = row.getAs("__transform_value");
        String sourceFilePath = row.getAs("__source_file_path");
        long position = row.getAs("__position");

        writer.add(
            LeafFileEntry.builder()
                .keyValue(keyValue)
                .transformValue(transformValue)
                .filePath(sourceFilePath)
                .position(position)
                .build());

        recordCount++;
        tvMin = Math.min(tvMin, transformValue);
        tvMax = Math.max(tvMax, transformValue);
      }
    }

    long sizeBytes = io.newInputFile(path).getLength();
    return java.util.Collections.singletonList(
            new LeafFileWriteResult(path, recordCount, sizeBytes, tvMin, tvMax))
        .iterator();
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  private InternalRow[] toOutputRows(BuildResult result) {
    InternalRow row =
        newInternalRow(
            org.apache.spark.unsafe.types.UTF8String.fromString(result.indexLocation),
            result.leafFileCount,
            result.recordCount);
    return new InternalRow[] {row};
  }

  /** Per-partition leaf-file write result, passed back to the driver via {@code
   * Encoders.javaSerialization} since it needs no further Spark-side column operations. */
  public static final class LeafFileWriteResult implements Serializable {
    private final String path;
    private final long recordCount;
    private final long sizeBytes;
    private final long transformValueMin;
    private final long transformValueMax;

    LeafFileWriteResult(
        String path, long recordCount, long sizeBytes, long transformValueMin,
        long transformValueMax) {
      this.path = path;
      this.recordCount = recordCount;
      this.sizeBytes = sizeBytes;
      this.transformValueMin = transformValueMin;
      this.transformValueMax = transformValueMax;
    }
  }

  private static final class BuildResult {
    private final String indexLocation;
    private final int leafFileCount;
    private final long recordCount;

    BuildResult(String indexLocation, int leafFileCount, long recordCount) {
      this.indexLocation = indexLocation;
      this.leafFileCount = leafFileCount;
      this.recordCount = recordCount;
    }
  }

  @Override
  public String description() {
    return "BuildScalarIndexProcedure";
  }
}
