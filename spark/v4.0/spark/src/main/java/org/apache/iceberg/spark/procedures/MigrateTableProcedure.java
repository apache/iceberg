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
import java.util.Map;
import org.apache.iceberg.actions.MigrateTable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.actions.MigrateTableSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class MigrateTableProcedure extends BaseProcedure {

  static final String NAME = "migrate";

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter PROPERTIES_PARAM =
      optionalInParameter("properties", STRING_MAP);
  private static final ProcedureParameter DROP_BACKUP_PARAM =
      optionalInParameter("drop_backup", DataTypes.BooleanType);
  private static final ProcedureParameter BACKUP_TABLE_NAME_PARAM =
      optionalInParameter("backup_table_name", DataTypes.StringType);
  private static final ProcedureParameter PARALLELISM_PARAM =
      optionalInParameter("parallelism", DataTypes.IntegerType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM, PROPERTIES_PARAM, DROP_BACKUP_PARAM, BACKUP_TABLE_NAME_PARAM, PARALLELISM_PARAM
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
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
    String tableName = input.asString(TABLE_PARAM, null);
    Preconditions.checkArgument(
        tableName != null && !tableName.isEmpty(),
        "Cannot handle an empty identifier for argument table");

    Map<String, String> properties = input.asStringMap(PROPERTIES_PARAM, ImmutableMap.of());

    boolean dropBackup = input.asBoolean(DROP_BACKUP_PARAM, false);
    String backupTableName = input.asString(BACKUP_TABLE_NAME_PARAM, null);

    MigrateTableSparkAction migrateTableSparkAction =
        SparkActions.get().migrateTable(tableName).tableProperties(properties);

    if (dropBackup) {
      migrateTableSparkAction = migrateTableSparkAction.dropBackup();
    }

    if (backupTableName != null) {
      migrateTableSparkAction = migrateTableSparkAction.backupTableName(backupTableName);
    }

    if (input.isProvided(PARALLELISM_PARAM)) {
      int parallelism = input.asInt(PARALLELISM_PARAM);
      Preconditions.checkArgument(parallelism > 0, "Parallelism should be larger than 0");
      migrateTableSparkAction =
          migrateTableSparkAction.executeWith(SparkTableUtil.migrationService(parallelism));
    }

    MigrateTable.Result result = migrateTableSparkAction.execute();
    return asScanIterator(OUTPUT_TYPE, newInternalRow(result.migratedDataFilesCount()));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "MigrateTableProcedure";
  }
}
