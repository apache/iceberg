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

import java.util.List;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/** A procedure that removes empty files from table's metadata. */
public class RemoveEmptyFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("empty_file_location", DataTypes.StringType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new Builder<RemoveEmptyFilesProcedure>() {
      @Override
      protected RemoveEmptyFilesProcedure doBuild() {
        return new RemoveEmptyFilesProcedure(tableCatalog());
      }
    };
  }

  private RemoveEmptyFilesProcedure(TableCatalog catalog) {
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
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    boolean dryRun = args.isNullAt(1) ? false : args.getBoolean(1);

    return withIcebergTable(
        tableIdent,
        table -> {
          List<String> emptyFiles =
              SparkTableUtil.loadMetadataTable(spark(), table, MetadataTableType.DATA_FILES)
                  .filter("record_count = 0")
                  .select("file_path")
                  .map((MapFunction<Row, String>) r -> r.get(0).toString(), Encoders.STRING())
                  .collectAsList();

          if (!dryRun) {
            DeleteFiles deleteFile = table.newDelete();
            emptyFiles.forEach(deleteFile::deleteFile);
            deleteFile.commit();
          }

          return toOutputRows(emptyFiles);
        });
  }

  private InternalRow[] toOutputRows(List<String> emptyFileLocations) {

    int orphanFileLocationsCount = Iterables.size(emptyFileLocations);
    InternalRow[] rows = new InternalRow[orphanFileLocationsCount];

    int index = 0;
    for (String fileLocation : emptyFileLocations) {
      rows[index] = newInternalRow(UTF8String.fromString(fileLocation));
      index++;
    }

    return rows;
  }

  @Override
  public String description() {
    return "RemoveEmptyFilesProcedure";
  }
}
