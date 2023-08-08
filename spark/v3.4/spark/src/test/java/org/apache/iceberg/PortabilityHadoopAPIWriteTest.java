/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class PortabilityHadoopAPIWriteTest {
    SparkSession spark;
    String warehouseLocation = "file:///opt/warehouse/my-namespace";
    PortabilityHadoopAPIWriteTest(String[] args){
        SparkConf conf = new SparkConf().
                setMaster("local").
                setAppName("IcebergWriteModeScenarios");

        this.spark = SparkSession.builder()
                .config(conf)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.adaptive.enabled","false")
                //.config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
        initializeHadoopCatalog("hadoop", warehouseLocation);
        //System.out.println(spark.conf().get("spark.eventLog.dir"));
    }

    public static void main(String[] args) {
        new PortabilityHadoopAPIWriteTest(args).run();
    }

    private void run(){
        Catalog catalog = getHadoopCatalog(warehouseLocation);
        TableIdentifier name = TableIdentifier.of("profile", "opportunity");
        Table table = catalog.loadTable(name);

        //read(table);
        write(table);
    }

    private void initializeHadoopCatalog(String catalogName, String warehouseLocation) {
        String catalogPrefix = "spark.sql.catalog." + catalogName;
        spark.conf().set(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog");
        spark.conf().set(catalogPrefix + ".warehouse", warehouseLocation);
        spark.conf().set(catalogPrefix + ".type", "hadoop");
    }

    private Catalog getHadoopCatalog(String warehouseLocation){
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), warehouseLocation);
        return hadoopCatalog;
    }

    private void read(Table table) {

        TableScan scan = table.newScan()
                .filter(Expressions.lessThan("device_id", 5))
                .select("device_id", "device_name");
        Schema projection = scan.schema();
        projection.columns().stream().forEach(System.out::println );
        Iterable<CombinedScanTask> tasks = scan.planTasks();
        for(CombinedScanTask combinedScanTask : tasks) {
            for(FileScanTask fileScanTask : combinedScanTask.files()) {
                System.out.println(fileScanTask.file().path());
            }
        }
    }
    private void write(Table table) {

        List list = table.currentSnapshot().allManifests(table.io());
        UnboundPredicate predicate = Expressions.equal("device_id", 5);
        //Transaction t = table.newDelete();
        DeleteFiles deleteFiles1 = table.newDelete();
        DeleteFiles deleteFiles2 = deleteFiles1.deleteFromRowFilter(predicate);
        deleteFiles2.commit();
    }

}
