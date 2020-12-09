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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.RemoveOrphanFilesAction;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A procedure that removes orphan files in a table.
 *
 * @see Actions#removeOrphanFiles()
 */
public class RemoveOrphanFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.optional("older_than", DataTypes.TimestampType),
      ProcedureParameter.optional("location", DataTypes.StringType),
      ProcedureParameter.optional("dry_run", DataTypes.BooleanType)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("orphan_file_location", DataTypes.StringType, false, Metadata.empty())
  });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RemoveOrphanFilesProcedure>() {
      @Override
      protected RemoveOrphanFilesProcedure doBuild() {
        return new RemoveOrphanFilesProcedure(tableCatalog());
      }
    };
  }

  private RemoveOrphanFilesProcedure(TableCatalog catalog) {
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
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    Long olderThanMillis = args.isNullAt(1) ? null : DateTimeUtils.toMillis(args.getLong(1));
    String location = args.isNullAt(2) ? null : args.getString(2);
    boolean dryRun = args.isNullAt(3) ? false : args.getBoolean(3);

    return withIcebergTable(tableIdent, table -> {
      SparkSession spark = SparkSession.active();
      Actions actions = Actions.forTable(spark, table);

      RemoveOrphanFilesAction action = actions.removeOrphanFiles();

      if (olderThanMillis != null) {
        boolean isTesting = Boolean.parseBoolean(spark.conf().get("spark.testing", "false"));
        if (!isTesting) {
          validateInterval(olderThanMillis);
        }
        action.olderThan(olderThanMillis);
      }

      if (location != null) {
        action.location(location);
      }

      if (dryRun) {
        action.deleteWith(file -> { });
      }

      List<String> orphanFileLocations = action.execute();

      InternalRow[] outputRows = new InternalRow[orphanFileLocations.size()];
      for (int index = 0; index < orphanFileLocations.size(); index++) {
        String fileLocation = orphanFileLocations.get(index);
        outputRows[index] = newInternalRow(UTF8String.fromString(fileLocation));
      }
      return outputRows;
    });
  }

  private void validateInterval(long olderThanMillis) {
    long intervalMillis = System.currentTimeMillis() - olderThanMillis;
    if (intervalMillis < TimeUnit.DAYS.toMillis(1)) {
      throw new IllegalArgumentException(
          "Cannot remove orphan files with an interval less than 24 hours. Executing this " +
          "procedure with a short interval may corrupt the table if other operations are happening " +
          "at the same time. If you are absolutely confident that no concurrent operations will be " +
          "affected by removing orphan files with such a short interval, you can use the Action API " +
          "to remove orphan files with an arbitrary interval.");
    }
  }

  @Override
  public String description() {
    return "RemoveOrphanFilesProcedure";
  }
}
