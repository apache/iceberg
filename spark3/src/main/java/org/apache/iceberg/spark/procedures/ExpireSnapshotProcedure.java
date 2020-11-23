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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ExpireSnapshotProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {
      ProcedureParameter.required("namespace", DataTypes.StringType),
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("older_than", DataTypes.TimestampType),
      ProcedureParameter.optional("retain_last", DataTypes.IntegerType)
  };


  private static final StructField[] OUTPUT_FIELDS = new StructField[] {
      new StructField("retain_last_num", DataTypes.IntegerType, true, Metadata.empty()),
      new StructField("expire_timestamp", DataTypes.TimestampType, true, Metadata.empty())
  };

  private static final StructType OUTPUT_TYPE = new StructType(OUTPUT_FIELDS);

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<ExpireSnapshotProcedure>() {
      @Override
      protected ExpireSnapshotProcedure doBuild() {
        return new ExpireSnapshotProcedure(tableCatalog());
      }
    };
  }

  private ExpireSnapshotProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public InternalRow[] call(InternalRow input) {
    String namespace = input.getString(0);
    String tableName = input.getString(1);
    Long timestamp = DateTimeUtils.toMillis(input.getLong(2));
    Integer retainLastNum = input.isNullAt(3) ? null : input.getInt(3);

    return modifyIcebergTable(namespace, tableName, table -> {
      if (retainLastNum == null) {
        table.expireSnapshots()
            .expireOlderThan(timestamp)
            .commit();
      } else {
        table.expireSnapshots()
            .expireOlderThan(timestamp)
            .retainLast(retainLastNum)
            .commit();
      }
      Object[] outputValues = new Object[OUTPUT_TYPE.size()];
      outputValues[0] = retainLastNum;
      outputValues[1] = timestamp;
      GenericInternalRow outputRow = new GenericInternalRow(outputValues);
      return new InternalRow[]{outputRow};
    });
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
  public String description() {
    return "ExpireSnapshotProcedure";
  }
}
