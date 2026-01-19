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

import java.util.Iterator;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
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

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter COLUMN_PARAM =
      requiredInParameter("column", DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, COLUMN_PARAM};

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
  public BoundProcedure bind(StructType inputType) {
    return this;
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public Iterator<Scan> call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    Identifier tableIdent = input.ident(TABLE_PARAM);
    String columnName = input.asString(COLUMN_PARAM, null);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          table.updateSchema().undeleteColumn(columnName).commit();

          // Get the restored field info
          Types.NestedField restoredField = table.schema().findField(columnName);

          InternalRow outputRow =
              newInternalRow(
                  UTF8String.fromString(columnName),
                  restoredField.fieldId(),
                  UTF8String.fromString(restoredField.type().toString()));
          return asScanIterator(OUTPUT_TYPE, outputRow);
        });
  }

  @Override
  public String name() {
    return "undelete_column";
  }

  @Override
  public String description() {
    return "UndeleteColumnProcedure";
  }
}
