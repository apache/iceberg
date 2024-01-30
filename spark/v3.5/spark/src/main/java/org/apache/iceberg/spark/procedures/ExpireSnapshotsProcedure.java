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

import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.actions.ExpireSnapshotsSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.util.DateTimeUtil;
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

/**
 * A procedure that expires snapshots in a table.
 *
 * @see SparkActions#expireSnapshots(Table)
 */
public class ExpireSnapshotsProcedure extends BaseProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("older_than", DataTypes.TimestampType),
        ProcedureParameter.optional("retain_last", DataTypes.IntegerType),
        ProcedureParameter.optional("max_concurrent_deletes", DataTypes.IntegerType),
        ProcedureParameter.optional("stream_results", DataTypes.BooleanType),
        ProcedureParameter.optional("snapshot_ids", DataTypes.createArrayType(DataTypes.LongType)),
        ProcedureParameter.optional("delete_files", DataTypes.BooleanType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("deleted_snapshots_count", DataTypes.LongType, true, Metadata.empty()),
            new StructField("deleted_data_files_count", DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                "deleted_position_delete_files_count", DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                "deleted_equality_delete_files_count", DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                "deleted_manifest_files_count", DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                "deleted_manifest_lists_count", DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                "deleted_statistics_files_count", DataTypes.LongType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<ExpireSnapshotsProcedure>() {
      @Override
      protected ExpireSnapshotsProcedure doBuild() {
        return new ExpireSnapshotsProcedure(tableCatalog());
      }
    };
  }

  private ExpireSnapshotsProcedure(TableCatalog tableCatalog) {
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
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    Long olderThanMillis = args.isNullAt(1) ? null : DateTimeUtil.microsToMillis(args.getLong(1));
    Integer retainLastNum = args.isNullAt(2) ? null : args.getInt(2);
    Integer maxConcurrentDeletes = args.isNullAt(3) ? null : args.getInt(3);
    Boolean streamResult = args.isNullAt(4) ? null : args.getBoolean(4);
    long[] snapshotIds = args.isNullAt(5) ? null : args.getArray(5).toLongArray();
    Boolean deleteFiles = args.isNullAt(6) ? null : args.getBoolean(6);

    Preconditions.checkArgument(
        maxConcurrentDeletes == null || maxConcurrentDeletes > 0,
        "max_concurrent_deletes should have value > 0, value: %s",
        maxConcurrentDeletes);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          ExpireSnapshots action = actions().expireSnapshots(table);

          if (olderThanMillis != null) {
            action.expireOlderThan(olderThanMillis);
          }

          if (retainLastNum != null) {
            action.retainLast(retainLastNum);
          }

          if (maxConcurrentDeletes != null) {
            if (table.io() instanceof SupportsBulkOperations) {
              LOG.warn(
                  "max_concurrent_deletes only works with FileIOs that do not support bulk deletes. This "
                      + "table is currently using {} which supports bulk deletes so the parameter will be ignored. "
                      + "See that IO's documentation to learn how to adjust parallelism for that particular "
                      + "IO's bulk delete.",
                  table.io().getClass().getName());
            } else {

              action.executeDeleteWith(executorService(maxConcurrentDeletes, "expire-snapshots"));
            }
          }

          if (snapshotIds != null) {
            for (long snapshotId : snapshotIds) {
              action.expireSnapshotId(snapshotId);
            }
          }

          if (streamResult != null) {
            action.option(
                ExpireSnapshotsSparkAction.STREAM_RESULTS, Boolean.toString(streamResult));
          }

          if (deleteFiles != null) {
            action.option(ExpireSnapshotsSparkAction.DELETE_FILES, Boolean.toString(deleteFiles));
          }

          ExpireSnapshots.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private InternalRow[] toOutputRows(ExpireSnapshots.Result result) {
    InternalRow row =
        newInternalRow(
            result.deletedSnapshotsCount(),
            result.deletedDataFilesCount(),
            result.deletedPositionDeleteFilesCount(),
            result.deletedEqualityDeleteFilesCount(),
            result.deletedManifestsCount(),
            result.deletedManifestListsCount(),
            result.deletedStatisticsFilesCount());
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "ExpireSnapshotsProcedure";
  }
}
