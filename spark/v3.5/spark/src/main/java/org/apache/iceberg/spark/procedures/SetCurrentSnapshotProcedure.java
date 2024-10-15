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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.BaseCatalog;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that sets the current snapshot in a table.
 *
 * <p><em>Note:</em> this procedure invalidates all cached Spark plans that reference the affected
 * table.
 *
 * @see org.apache.iceberg.ManageSnapshots#setCurrentSnapshot(long)
 */
class SetCurrentSnapshotProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("snapshot_id", DataTypes.LongType),
        ProcedureParameter.optional("ref", DataTypes.StringType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("previous_snapshot_id", DataTypes.LongType, true, Metadata.empty()),
            new StructField("current_snapshot_id", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<SetCurrentSnapshotProcedure>() {
      @Override
      protected SetCurrentSnapshotProcedure doBuild() {
        return new SetCurrentSnapshotProcedure(catalog());
      }
    };
  }

  private SetCurrentSnapshotProcedure(BaseCatalog catalog) {
    super(catalog);
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
    Long snapshotId = args.isNullAt(1) ? null : args.getLong(1);
    String ref = args.isNullAt(2) ? null : args.getString(2);
    Preconditions.checkArgument(
        (snapshotId != null && ref == null) || (snapshotId == null && ref != null),
        "Either snapshot_id or ref must be provided, not both");

    return modifyIcebergTable(
        tableIdent,
        table -> {
          Snapshot previousSnapshot = table.currentSnapshot();
          Long previousSnapshotId = previousSnapshot != null ? previousSnapshot.snapshotId() : null;

          long targetSnapshotId = snapshotId != null ? snapshotId : toSnapshotId(table, ref);
          table.manageSnapshots().setCurrentSnapshot(targetSnapshotId).commit();

          InternalRow outputRow = newInternalRow(previousSnapshotId, targetSnapshotId);
          return new InternalRow[] {outputRow};
        });
  }

  @Override
  public String description() {
    return "SetCurrentSnapshotProcedure";
  }

  private long toSnapshotId(Table table, String refName) {
    SnapshotRef ref = table.refs().get(refName);
    ValidationException.check(ref != null, "Cannot find matching snapshot ID for ref " + refName);
    return ref.snapshotId();
  }
}
