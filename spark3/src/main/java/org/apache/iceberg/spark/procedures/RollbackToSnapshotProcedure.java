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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RollbackToSnapshotProcedure extends BaseProcedure {

  private final ProcedureParameter[] parameters = new ProcedureParameter[]{
      ProcedureParameter.required("namespace", DataTypes.StringType),
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("snapshot_id", DataTypes.LongType)
  };
  private final StructField[] outputFields = new StructField[]{
      new StructField("previous_current_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
      new StructField("current_snapshot_id", DataTypes.LongType, false, Metadata.empty())
  };
  private final StructType outputType = new StructType(outputFields);

  public RollbackToSnapshotProcedure(TableCatalog catalog) {
    super(catalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return parameters;
  }

  @Override
  public StructType outputType() {
    return outputType;
  }

  @Override
  public InternalRow[] call(InternalRow input) {
    String namespace = input.getString(0);
    String tableName = input.getString(1);
    long snapshotId = input.getLong(2);

    return modifyIcebergTable(namespace, tableName, table -> {
      Snapshot previousCurrentSnapshot = table.currentSnapshot();

      table.manageSnapshots()
          .rollbackTo(snapshotId)
          .commit();

      Object[] outputValues = new Object[outputFields.length];
      outputValues[0] = previousCurrentSnapshot.snapshotId();
      outputValues[1] = snapshotId;
      GenericInternalRow outputRow = new GenericInternalRow(outputValues);

      return new InternalRow[] {outputRow};
    });
  }

  @Override
  public String description() {
    return "RollbackToSnapshotProcedure";
  }
}
