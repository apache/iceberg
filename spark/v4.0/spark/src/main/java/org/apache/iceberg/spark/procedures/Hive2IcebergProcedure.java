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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hive2IcebergProcedure is responsible for migrating a regular Hive table to an Iceberg table. */
public class Hive2IcebergProcedure extends BaseProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(Hive2IcebergProcedure.class);

  private static final ProcedureParameter SOURCE_TABLE_PARAM =
      requiredInParameter("source_table", DataTypes.StringType);
  private static final ProcedureParameter PARALLELISM =
      optionalInParameter("parallelism", DataTypes.IntegerType);

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("imported_files_count", DataTypes.LongType, false, Metadata.empty())
          });

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {SOURCE_TABLE_PARAM, PARALLELISM};

  protected Hive2IcebergProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return new ProcedureParameter[0];
  }

  @Override
  public Iterator<Scan> call(InternalRow input) {
    return null;
  }

  @Override
  public BoundProcedure bind(StructType inputType) {
    return null;
  }

  @Override
  public String name() {
    return "";
  }
}
