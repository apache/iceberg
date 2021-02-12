/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.actions.compaction;

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.SparkFileScan;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.spark.source.SparkWriteBuilder;
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

public class SparkBinningCompactionStrategy extends BinningCompactionStrategy {

  private final SparkSession spark;

  public SparkBinningCompactionStrategy(SparkSession spark) {
    this.spark = spark;
  }

  @Override
  public List<DataFile> rewriteFiles(Table table, List<FileScanTask> filesToRewrite, String description) {
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
    BatchWrite write = writeBuilder.overwriteFiles(scan, IsolationLevel.SERIALIZABLE).buildForBatch();

    WriteToDataSourceV2 writePlan = WriteToDataSourceV2$.MODULE$.apply(
        write,
        DataSourceV2ScanRelation$.MODULE$.apply(sparkTable, scan, sparkSchema.toAttributes()));

    // I think there is a better way to do this
    // Add description to query name?
    // One major downside to this is we don't how many files were made by this
    InternalRow[] results = spark.sessionState().executePlan(writePlan).executedPlan().executeCollect();
    // Maybe search through snapshots for one that matches this operation
    // sparkTable.table().snapshots().f

    return null;
  }
}
