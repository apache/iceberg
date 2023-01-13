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

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.apache.hadoop.shaded.com.google.common.collect.ImmutableList.toImmutableList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A procedure that gets approximate NDV (number of distinct value) for the requested columns
 * and sets this to the table's StatisticsFile.
 */
public class DistinctCountProcedure extends BaseProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(DistinctCountProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("distinct_count_view", DataTypes.StringType),
        ProcedureParameter.optional("columns", STRING_ARRAY),
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("view_name", DataTypes.StringType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<DistinctCountProcedure>() {
      @Override
      protected DistinctCountProcedure doBuild() {
        return new DistinctCountProcedure(tableCatalog());
      }
    };
  }

  private DistinctCountProcedure(TableCatalog tableCatalog) {
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
    String tableName = args.getString(0);
    Identifier tableIdent = toIdentifier(tableName, PARAMETERS[0].name());
    SparkTable sparkTable = loadSparkTable(tableIdent);
    StructType schema = sparkTable.schema();
    Table table = sparkTable.table();
    ArrayData columns = args.getArray(2);
    int columnSizes = columns.numElements();

    long[] ndvs = new long[columnSizes];
    int[] fieldId = new int[columnSizes];
    String query = "SELECT ";
    for (int i = 0; i < columnSizes; i++) {
      String colName = columns.getUTF8String(i).toString();
      query += "APPROX_COUNT_DISTINCT(" + colName + "), ";
      fieldId[i] = schema.fieldIndex(colName);
    }

    query = query.substring(0, query.length() - 2) + " FROM " + tableName;
    Dataset<Row> df = spark().sql(query);

    for (int i = 0; i < columnSizes; i++) {
      ndvs[i] = df.head().getLong(i);
    }

    TableOperations operations = ((HasTableOperations) table).operations();
    FileIO fileIO = ((HasTableOperations) table).operations().io();
    String path = operations.metadataFileLocation(format("%s.stats", randomUUID()));
    OutputFile outputFile = fileIO.newOutputFile(path);

    try (PuffinWriter writer =
        Puffin.write(outputFile).createdBy("Spark DistinctCountProcedure").build()) {
      for (int i = 0; i < columnSizes; i++) {
        writer.add(
            new Blob(
                StandardBlobTypes.NDV_BLOB,
                ImmutableList.of(fieldId[i]),
                table.currentSnapshot().snapshotId(),
                table.currentSnapshot().sequenceNumber(),
                ByteBuffer.allocate(0),
                null,
                ImmutableMap.of("ndv", Long.toString(ndvs[i]))));
      }
      writer.finish();

      GenericStatisticsFile statisticsFile =
          new GenericStatisticsFile(
              table.currentSnapshot().snapshotId(),
              path,
              writer.fileSize(),
              writer.footerSize(),
              writer.writtenBlobsMetadata().stream()
                  .map(GenericBlobMetadata::from)
                  .collect(toImmutableList()));

      table
          .updateStatistics()
          .setStatistics(table.currentSnapshot().snapshotId(), statisticsFile)
          .commit();
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }

    String viewName = viewName(args, tableName);
    // Create a view for users to query
    df.createOrReplaceTempView(viewName);
    return toOutputRows(viewName);
  }

  @NotNull
  private static String viewName(InternalRow args, String tableName) {
    String viewName = args.isNullAt(1) ? null : args.getString(1);
    if (viewName == null) {
      String shortTableName =
          tableName.contains(".") ? tableName.substring(tableName.lastIndexOf(".") + 1) : tableName;
      viewName = shortTableName + "_changes";
    }
    return viewName;
  }

  private InternalRow[] toOutputRows(String viewName) {
    InternalRow row = newInternalRow(UTF8String.fromString(viewName));
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "DistinctCountProcedure";
  }
}
