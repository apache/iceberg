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

import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.spark.actions.RewriteTablePathSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class RewriteTablePathProcedure extends BaseProcedure {

  private static final ProcedureParameter TABLE_PARAM =
      ProcedureParameter.required("table", DataTypes.StringType);
  private static final ProcedureParameter SOURCE_PREFIX_PARAM =
      ProcedureParameter.required("source_prefix", DataTypes.StringType);
  private static final ProcedureParameter TARGET_PREFIX_PARAM =
      ProcedureParameter.required("target_prefix", DataTypes.StringType);
  private static final ProcedureParameter START_VERSION_PARAM =
      ProcedureParameter.optional("start_version", DataTypes.StringType);
  private static final ProcedureParameter END_VERSION_PARM =
      ProcedureParameter.optional("end_version", DataTypes.StringType);
  private static final ProcedureParameter STAGING_LOCATION_PARAM =
      ProcedureParameter.optional("staging_location", DataTypes.StringType);
  private static final ProcedureParameter CREATE_FILE_LIST_PARAM =
      ProcedureParameter.optional("create_file_list", DataTypes.BooleanType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        SOURCE_PREFIX_PARAM,
        TARGET_PREFIX_PARAM,
        START_VERSION_PARAM,
        END_VERSION_PARM,
        STAGING_LOCATION_PARAM,
        CREATE_FILE_LIST_PARAM
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("latest_version", DataTypes.StringType, true, Metadata.empty()),
            new StructField("file_list_location", DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                "rewritten_manifest_file_paths_count",
                DataTypes.IntegerType,
                true,
                Metadata.empty()),
            new StructField(
                "rewritten_delete_file_paths_count", DataTypes.IntegerType, true, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RewriteTablePathProcedure>() {
      @Override
      protected RewriteTablePathProcedure doBuild() {
        return new RewriteTablePathProcedure(tableCatalog());
      }
    };
  }

  private RewriteTablePathProcedure(TableCatalog tableCatalog) {
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
    String sourcePrefix = input.asString(SOURCE_PREFIX_PARAM);
    String targetPrefix = input.asString(TARGET_PREFIX_PARAM);
    String startVersion = input.asString(START_VERSION_PARAM, null);
    String endVersion = input.asString(END_VERSION_PARM, null);
    String stagingLocation = input.asString(STAGING_LOCATION_PARAM, null);
    boolean createFileList = input.asBoolean(CREATE_FILE_LIST_PARAM, true);

    return withIcebergTable(
        tableIdent,
        table -> {
          RewriteTablePathSparkAction action = SparkActions.get().rewriteTablePath(table);

          if (startVersion != null) {
            action.startVersion(startVersion);
          }
          if (endVersion != null) {
            action.endVersion(endVersion);
          }
          if (stagingLocation != null) {
            action.stagingLocation(stagingLocation);
          }

          action.createFileList(createFileList);
          return toOutputRows(action.rewriteLocationPrefix(sourcePrefix, targetPrefix).execute());
        });
  }

  private InternalRow[] toOutputRows(RewriteTablePath.Result result) {
    return new InternalRow[] {
      newInternalRow(
          UTF8String.fromString(result.latestVersion()),
          UTF8String.fromString(result.fileListLocation()),
          result.rewrittenManifestFilePathsCount(),
          result.rewrittenDeleteFilePathsCount())
    };
  }

  @Override
  public String description() {
    return "RewriteTablePathProcedure";
  }
}
