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

import java.util.Map;
import org.apache.iceberg.actions.CreateAction;
import org.apache.iceberg.actions.Spark3MigrateAction;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

class MigrateTableProcedure extends BaseProcedure {
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.optional("properties", STRING_MAP)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("migrated_files_count", DataTypes.LongType, false, Metadata.empty())
  });

  private MigrateTableProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<MigrateTableProcedure>() {
      @Override
      protected MigrateTableProcedure doBuild() {
        return new MigrateTableProcedure(tableCatalog());
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
    String tableName = args.getString(0);
    CatalogAndIdentifier catalogAndIdent = toCatalogAndIdentifier(tableName, PARAMETERS[0].name(), tableCatalog());

    Map<String, String> properties = Maps.newHashMap();
    if (!args.isNullAt(1)) {
      args.getMap(1).foreach(DataTypes.StringType, DataTypes.StringType,
          (k, v) -> {
            properties.put(k.toString(), v.toString());
            return BoxedUnit.UNIT;
          });
    }

    CreateAction action = new Spark3MigrateAction(spark(), catalogAndIdent.catalog(), catalogAndIdent.identifier());

    long numMigratedFiles = action.withProperties(properties).execute();
    return new InternalRow[] {newInternalRow(numMigratedFiles)};
  }

  @Override
  public String description() {
    return "MigrateTableProcedure";
  }
}
