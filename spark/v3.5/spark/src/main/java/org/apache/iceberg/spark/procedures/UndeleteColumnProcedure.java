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
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A procedure that restores a previously deleted column from the schema history.
 *
 * <p>The column is restored with its original field ID, preserving data file compatibility.
 *
 * @see org.apache.iceberg.UpdateSchema#undeleteColumn(String)
 */
class UndeleteColumnProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.required("column", DataTypes.StringType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("column_name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("field_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("type", DataTypes.StringType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<UndeleteColumnProcedure>() {
      @Override
      protected UndeleteColumnProcedure doBuild() {
        return new UndeleteColumnProcedure(tableCatalog());
      }
    };
  }

  private UndeleteColumnProcedure(TableCatalog tableCatalog) {
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
    String columnName = args.getString(1);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          table.updateSchema().undeleteColumn(columnName).commit();

          // Get the restored field info
          Types.NestedField restoredField = table.schema().findField(columnName);

          return new InternalRow[] {
            newInternalRow(
                UTF8String.fromString(columnName),
                restoredField.fieldId(),
                UTF8String.fromString(restoredField.type().toString()))
          };
        });
  }

  @Override
  public String description() {
    return "UndeleteColumnProcedure";
  }
}
