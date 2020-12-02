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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.actions.CreateAction;
import org.apache.iceberg.actions.Spark3MigrateAction;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

class MigrateProcedure extends BaseProcedure {
  private static final DataType MAP = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.optional("table_options", MAP)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("num_datafiles_included", DataTypes.LongType, false, Metadata.empty())
  });

  private MigrateProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<MigrateProcedure>() {
      @Override
      protected MigrateProcedure doBuild() {
        return new MigrateProcedure(tableCatalog());
      }
    };
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
    Map<String, String> options = new HashMap<>();
    if (!args.isNullAt(1)) {
      args.getMap(1).foreach(DataTypes.StringType, DataTypes.StringType,
          (k, v) -> {
            options.put(k.toString(), v.toString());
            return BoxedUnit.UNIT;
          });
    }

    String tableName = args.getString(0);
    CatalogAndIdentifier tableIdent = toCatalogAndIdentifer(tableName, PARAMETERS[0].name(), tableCatalog());
    CreateAction action =  new Spark3MigrateAction(spark(), tableIdent.catalog(), tableIdent.identifier());

    long numFiles = action.withProperties(options).execute();
    return new InternalRow[] {newInternalRow(numFiles)};
  }

  @Override
  public String description() {
    return "Migrate a Spark table to Iceberg. The identifier for the table will not change but all files will now" +
        "be read and written through Iceberg.";
  }
}
