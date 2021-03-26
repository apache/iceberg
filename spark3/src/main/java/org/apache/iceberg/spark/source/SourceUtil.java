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

package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.LogicalWriteInfoImpl$;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation$;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SourceUtil {

  private SourceUtil() {
  }

  public static List<DataFile> rewriteFiles(SparkSession spark, Table table, List<FileScanTask> filesToRewrite,
                                            long targetSize) {

    SparkTable sparkTable = new SparkTable(table, true);
    StructType sparkSchema = sparkTable.schema();

    // Build Read
    Scan scan = SparkFileScan.scanOfFiles(spark, table, filesToRewrite, targetSize);

    // Build Overwrite from Read
    String queryID = UUID.randomUUID().toString();
    LogicalWriteInfo info = LogicalWriteInfoImpl$.MODULE$.apply(
        queryID, sparkSchema, CaseInsensitiveStringMap.empty());
    SparkWriteBuilder writeBuilder = new SparkWriteBuilder(spark, table, info);
    // Do we make this configurable or use the table property? Maybe best to not let this be changeable ...
    SparkWrite.CopyOnWriteMergeWrite write = (SparkWrite.CopyOnWriteMergeWrite)
        writeBuilder.overwriteFiles(scan, IsolationLevel.SERIALIZABLE).buildForBatch();

    WriteToDataSourceV2 writePlan = WriteToDataSourceV2$.MODULE$.apply(
        write,
        DataSourceV2ScanRelation$.MODULE$.apply(sparkTable, scan, sparkSchema.toAttributes()));

    return write.writtenFiles();
  }
}
