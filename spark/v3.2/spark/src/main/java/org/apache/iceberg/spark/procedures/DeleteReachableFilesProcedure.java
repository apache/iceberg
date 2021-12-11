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

import org.apache.iceberg.actions.DeleteReachableFiles;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that deletes all reachable files for given metadata location.
 *
 * @see SparkActions#deleteReachableFiles(String)
 */
public class DeleteReachableFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {
      ProcedureParameter.required("metadataFileLocation", DataTypes.StringType)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("deleted_data_files_count", DataTypes.LongType, true, Metadata.empty()),
      new StructField("deleted_manifest_files_count", DataTypes.LongType, true, Metadata.empty()),
      new StructField("deleted_manifest_lists_count", DataTypes.LongType, true, Metadata.empty()),
      new StructField("deleted_other_files_count", DataTypes.LongType, true, Metadata.empty())
  });

  public static ProcedureBuilder builder() {
    return new Builder<DeleteReachableFilesProcedure>() {
      @Override
      protected DeleteReachableFilesProcedure doBuild() {
        return new DeleteReachableFilesProcedure(tableCatalog());
      }
    };
  }

  private DeleteReachableFilesProcedure(TableCatalog catalog) {
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
    return toOutputRows(actions().deleteReachableFiles(args.getString(0)).execute());
  }

  private InternalRow[] toOutputRows(DeleteReachableFiles.Result result) {
    InternalRow row = newInternalRow(
        result.deletedDataFilesCount(),
        result.deletedManifestsCount(),
        result.deletedManifestListsCount(),
        result.deletedOtherFilesCount()
    );
    return new InternalRow[]{row};
  }

  @Override
  public String description() {
    return "DeleteReachableFilesProcedure";
  }
}
