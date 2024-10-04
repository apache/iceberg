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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A procedure that adds back files to a table that were removed in a given delete operation. */
class RevertDeleteProcedure extends BaseProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(RevertDeleteProcedure.class);
  private static final String RECOVERED_FROM_PROP = "recovered-from-snapshot-id";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.required("snapshot_id", DataTypes.LongType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType)
      };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("added_files_count", DataTypes.LongType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RevertDeleteProcedure>() {
      @Override
      protected RevertDeleteProcedure doBuild() {
        return new RevertDeleteProcedure(tableCatalog());
      }
    };
  }

  private RevertDeleteProcedure(TableCatalog tableCatalog) {
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
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    long snapshotId = args.getLong(1);
    boolean dryRun = !args.isNullAt(2) && args.getBoolean(2);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          long count = revertDelete(table, snapshotId, dryRun);
          InternalRow row = newInternalRow(count);
          return new InternalRow[] {row};
        });
  }

  public long revertDelete(Table table, long snapshotId, boolean dryRun) {
    Snapshot snapshot = findSnapshot(table, snapshotId);

    checkSnapshotIsDelete(table, snapshot);

    checkForPreviousRevertDelete(table, snapshot.snapshotId());

    AppendFiles append = table.newAppend();
    long count = appendFilesPreviouslyRemoved(table, snapshot, append);

    if (dryRun) {
      LOG.debug("Dry run, would have appended {} files back to table {}", count, table.name());
    } else {
      append.commit();
      LOG.debug("Committed {} files back to table {}", count, table.name());
    }

    return count;
  }

  private Snapshot findSnapshot(Table table, long snapshotId) {
    Snapshot snapshot = table.snapshot(snapshotId);
    Preconditions.checkNotNull(
        snapshot, "Snapshot %s in table %s was not found", snapshotId, table.name());

    Preconditions.checkState(
        SnapshotUtil.isAncestorOf(table, snapshotId),
        "Snapshot %s in table %s is not an ancestor of the current table state",
        snapshotId,
        table.name());

    return snapshot;
  }

  private void checkSnapshotIsDelete(Table table, Snapshot snapshot) {
    // only allow delete operations...
    Preconditions.checkArgument(
        snapshot.operation().equals(DataOperations.DELETE),
        "Snapshot %s in table %s is not a delete operation",
        snapshot.snapshotId(),
        table.name());

    // Ensure that the snapshot does not involve delete files
    String addedDeleteFiles =
        snapshot.summary().getOrDefault(SnapshotSummary.ADDED_DELETE_FILES_PROP, "0");
    String removedDeleteFiles =
        snapshot.summary().getOrDefault(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "0");
    Preconditions.checkArgument(
        "0".equals(addedDeleteFiles) && "0".equals(removedDeleteFiles),
        "Snapshot %s in table %s involves delete files, which is not supported",
        snapshot.snapshotId(),
        table.name());
  }

  private void checkForPreviousRevertDelete(Table table, long snapshotId) {
    // check for a previous revert delete, to prevent duplicate file entries...
    table
        .snapshots()
        .iterator()
        .forEachRemaining(
            snap -> {
              Preconditions.checkState(
                  !snap.summary()
                      .getOrDefault(RECOVERED_FROM_PROP, "")
                      .equals(Long.toString(snapshotId)),
                  "Snapshot %s in table %s has already been reverted",
                  snapshotId,
                  table.name());
            });
  }

  private long appendFilesPreviouslyRemoved(Table table, Snapshot snapshot, AppendFiles append) {
    append.set(RECOVERED_FROM_PROP, Long.toString(snapshot.snapshotId()));

    AtomicLong counter = new AtomicLong();
    snapshot
        .removedDataFiles(table.io())
        .forEach(
            file -> {
              Preconditions.checkState(
                  table.io().newInputFile(file.path().toString()).exists(),
                  "File %s from table %s does not exist",
                  file.path(),
                  table.name());
              LOG.debug("Appending file {} back to table {}", file.path(), table.name());
              append.appendFile(file);
              counter.incrementAndGet();
            });
    return counter.get();
  }

  @Override
  public String description() {
    return "RevertDeleteProcedure";
  }
}
