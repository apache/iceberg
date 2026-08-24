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

import java.util.Objects;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UndeleteUtils;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that restores a dropped column under its original field id.
 *
 * <p><em>Note:</em> this procedure invalidates all cached Spark plans that reference the affected
 * table.
 *
 * @see UpdateSchema#undeleteColumn(String)
 */
class UndeleteProcedure extends BaseProcedure {

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("restored_field_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("applied_schema_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("wrote_during_window", DataTypes.BooleanType, false, Metadata.empty()),
            new StructField("was_identifier", DataTypes.BooleanType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<UndeleteProcedure>() {
      @Override
      public UndeleteProcedure doBuild() {
        return new UndeleteProcedure(tableCatalog());
      }
    };
  }

  private UndeleteProcedure(TableCatalog tableCatalog) {
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
    String column = input.asString(COLUMN_PARAM);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          TableMetadata current = ((BaseTable) table).operations().current();
          Types.NestedField deletedColumn =
              UndeleteUtils.findDeletedColumn(current.schemas(), column);
          // conservative: reports writes during the window unless lineage proves none happened
          boolean wroteDuringWindow =
              deletedColumn != null
                  && UndeleteUtils.newestContainingSnapshotIndex(current, deletedColumn.fieldId())
                      != 0;
          // the restored field is never re-registered as an identifier automatically
          boolean wasIdentifier = false;
          if (deletedColumn != null) {
            Schema winningSchema =
                UndeleteUtils.findWinningSchema(
                    current.schemas(), column, deletedColumn.fieldId());
            wasIdentifier =
                winningSchema != null
                    && winningSchema.identifierFieldIds().contains(deletedColumn.fieldId());
          }

          table.updateSchema().undeleteColumn(column).commit();

          Schema committedSchema = table.schema();
          InternalRow outputRow =
              newInternalRow(
                  Objects.requireNonNull(
                          committedSchema.findField(column),
                          "Cannot find undeleted column in committed schema: " + column)
                      .fieldId(),
                  committedSchema.schemaId(),
                  wroteDuringWindow,
                  wasIdentifier);
          return new InternalRow[] {outputRow};
        });
  }

  @Override
  public String description() {
    return "UndeleteProcedure";
  }
}
