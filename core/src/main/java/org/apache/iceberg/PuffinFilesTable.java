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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * A {@link Table} implementation that exposes Puffin files associated with a selected snapshot.
 *
 * <p>Deletion vector files are derived from the selected snapshot's live delete manifests.
 * Statistics files are derived from the statistics files currently registered in table metadata for
 * the selected snapshot.
 *
 * <p>Statistics registration may be updated independently of snapshots. Therefore, time travel
 * queries return the statistics files currently associated with the selected snapshot, rather than
 * the statistics registration state at the time the snapshot was committed.
 *
 * <p>Each row represents a Puffin file associated with the selected snapshot through one metadata
 * source. A physical Puffin file may appear more than once if it is associated through multiple
 * sources.
 */
public class PuffinFilesTable extends BaseMetadataTable {

  private static final String SOURCE_COLUMN = "source";

  private static final List<String> DV_SCAN_COLUMNS =
      ImmutableList.of(
          DataFile.FILE_PATH.name(),
          DataFile.FILE_FORMAT.name(),
          DataFile.FILE_SIZE.name(),
          DataFile.CONTENT_OFFSET.name(),
          DataFile.CONTENT_SIZE.name());

  private static final Schema PUFFIN_FILES_SCHEMA =
      new Schema(
          Types.NestedField.required(
              1, "snapshot_id", Types.LongType.get(), "ID of the selected snapshot"),
          Types.NestedField.required(
              2,
              "file_path",
              Types.StringType.get(),
              "Fully qualified location of the Puffin file"),
          Types.NestedField.required(
              3,
              "source",
              Types.StringType.get(),
              "Metadata source that associates the Puffin file with the selected snapshot"),
          Types.NestedField.required(
              4,
              "file_size_in_bytes",
              Types.LongType.get(),
              "Total size of the Puffin file in bytes"),
          Types.NestedField.required(
              5,
              "referenced_blob_count",
              Types.IntegerType.get(),
              "Number of distinct blobs in the Puffin file referenced by the selected snapshot"),
          Types.NestedField.required(
              6,
              "referenced_blob_types",
              Types.ListType.ofRequired(7, Types.StringType.get()),
              "Distinct types of blobs referenced by the selected snapshot"),
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
              "Distinct fields referenced across the selected blobs"));

  PuffinFilesTable(Table table) {
    this(table, table.name() + ".puffin_files");
  }

  PuffinFilesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new PuffinFilesTableScan(table());
  }

  @Override
  public Schema schema() {
    return PUFFIN_FILES_SCHEMA;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.PUFFIN_FILES;
  }

  private DataTask task(BaseTableScan scan) {
    Snapshot snapshot = scan.snapshot();
    Preconditions.checkState(snapshot != null, "Cannot plan Puffin files without a snapshot");

    Schema baseTableSchema = table().schema();
    Evaluator evaluator = new Evaluator(schema().asStruct(), scan.filter(), scan.isCaseSensitive());
    SourceEvaluator sourceEvaluator =
        new SourceEvaluator(scan.filter(), schema().asStruct(), scan.isCaseSensitive());

    List<PuffinFileReferences.PuffinFileReference> matchingFiles =
        puffinFiles(snapshot, sourceEvaluator).stream()
            .filter(
                puffinFile ->
                    evaluator.eval(PuffinFileReferences.toRow(baseTableSchema, puffinFile)))
            .collect(ImmutableList.toImmutableList());

    return StaticDataTask.of(
        table().io().newInputFile(taskLocation(snapshot)),
        schema(),
        scan.schema(),
        matchingFiles,
        puffinFile -> PuffinFileReferences.toRow(baseTableSchema, puffinFile));
  }

  private String taskLocation(Snapshot snapshot) {
    if (snapshot.manifestListLocation() != null) {
      return snapshot.manifestListLocation();
    }

    String metadataFileLocation = table().operations().current().metadataFileLocation();
    Preconditions.checkState(
        metadataFileLocation != null,
        "Cannot determine a metadata file location for Puffin files table %s",
        name());

    return metadataFileLocation;
  }

  private List<PuffinFileReferences.PuffinFileReference> puffinFiles(
      Snapshot snapshot, SourceEvaluator sourceEvaluator) {
    ImmutableList.Builder<PuffinFileReferences.PuffinFileReference> rows = ImmutableList.builder();
    long snapshotId = snapshot.snapshotId();

    if (sourceEvaluator.mayMatch(PuffinFileReferences.SOURCE_STATISTICS)) {
      for (StatisticsFile statisticsFile : table().statisticsFiles()) {
        if (statisticsFile.snapshotId() == snapshotId) {
          rows.add(PuffinFileReferences.fromStatisticsFile(snapshotId, statisticsFile));
        }
      }
    }

    if (sourceEvaluator.mayMatch(PuffinFileReferences.SOURCE_DELETION_VECTOR)) {
      rows.addAll(deletionVectorPuffinFiles(snapshot));
    }

    return rows.build();
  }

  private List<PuffinFileReferences.PuffinFileReference> deletionVectorPuffinFiles(
      Snapshot snapshot) {
    Map<String, PuffinFileReferences.DeletionVectorFileAccumulator> dvFiles =
        Maps.newLinkedHashMap();
    long snapshotId = snapshot.snapshotId();

    for (ManifestFile deleteManifest : snapshot.deleteManifests(table().io())) {
      try (ManifestReader<DeleteFile> reader =
              ManifestFiles.readDeleteManifest(deleteManifest, table().io(), table().specs())
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
              "Missing content offset for deletion vector in Puffin file: %s",
              path);
          Preconditions.checkState(
              contentSizeInBytes != null,
              "Missing content size for deletion vector in Puffin file: %s",
              path);

          dvFiles
              .computeIfAbsent(
                  path,
                  ignored ->
                      new PuffinFileReferences.DeletionVectorFileAccumulator(
                          snapshotId, path, deleteFile.fileSizeInBytes()))
              .add(deleteFile.fileSizeInBytes(), contentOffset, contentSizeInBytes);
        }

      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to read delete manifest: %s", deleteManifest.path());
      }
    }

    return dvFiles.values().stream()
        .map(PuffinFileReferences.DeletionVectorFileAccumulator::toPuffinFileReference)
        .collect(ImmutableList.toImmutableList());
  }

  /**
   * Conservatively evaluates an {@link Expression} using a Puffin file source known at planning
   * time.
   */
  private static class SourceEvaluator {
    private final Expression boundExpr;

    private SourceEvaluator(Expression expr, Types.StructType structType, boolean caseSensitive) {
      Expression rewritten = Expressions.rewriteNot(expr);
      this.boundExpr = Binder.bind(structType, rewritten, caseSensitive);
    }

    private boolean mayMatch(String source) {
      return new SourceEvalVisitor().eval(source);
    }

    private class SourceEvalVisitor extends ExpressionVisitors.BoundExpressionVisitor<Boolean> {
      private static final boolean ROWS_MIGHT_MATCH = true;
      private static final boolean ROWS_CANNOT_MATCH = false;

      private String source;

      private boolean eval(String puffinSource) {
        this.source = puffinSource;
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
        if (sourceColumn(ref)) {
          return ROWS_CANNOT_MATCH; // source should not be null
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
        if (sourceColumn(ref)) {
          return ROWS_CANNOT_MATCH; // source should not be nan
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
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
        if (sourceColumn(ref)) {
          Literal<CharSequence> stringLit = lit.to(Types.StringType.get());
          if (!sourceMatch(stringLit.value())) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
        if (sourceColumn(ref)) {
          Literal<CharSequence> stringLit = lit.to(Types.StringType.get());
          if (sourceMatch(stringLit.value())) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
        if (sourceColumn(ref)) {
          if (literalSet.stream().noneMatch(this::sourceMatch)) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
        if (sourceColumn(ref)) {
          if (literalSet.stream().anyMatch(this::sourceMatch)) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
        if (sourceColumn(ref)) {
          Literal<CharSequence> stringLit = lit.to(Types.StringType.get());
          if (!source.startsWith(stringLit.value().toString())) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      @Override
      public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
        if (sourceColumn(ref)) {
          Literal<CharSequence> stringLit = lit.to(Types.StringType.get());
          if (source.startsWith(stringLit.value().toString())) {
            return ROWS_CANNOT_MATCH;
          }
        }
        return ROWS_MIGHT_MATCH;
      }

      private <T> boolean sourceColumn(BoundReference<T> ref) {
        return ref.fieldId() == PUFFIN_FILES_SCHEMA.findField(SOURCE_COLUMN).fieldId();
      }

      private boolean sourceMatch(Object value) {
        return value instanceof CharSequence && source.contentEquals((CharSequence) value);
      }
    }
  }

  private class PuffinFilesTableScan extends StaticTableScan {

    PuffinFilesTableScan(Table table) {
      super(
          table, PUFFIN_FILES_SCHEMA, MetadataTableType.PUFFIN_FILES, PuffinFilesTable.this::task);
    }

    PuffinFilesTableScan(Table table, TableScanContext context) {
      super(
          table,
          PUFFIN_FILES_SCHEMA,
          MetadataTableType.PUFFIN_FILES,
          PuffinFilesTable.this::task,
          context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new PuffinFilesTableScan(table, context);
    }
  }
}
