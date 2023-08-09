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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.IcebergSpark;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.col;

public class PopulateData {
    SparkSession spark;
    String warehouseLocation = "file:///opt/warehouse/my-namespace";
    int bucketSize = 2;

    PopulateData(String[] args){
        SparkConf conf = new SparkConf().
                setMaster("local").
                setAppName("IcebergWriteModeScenarios");

        this.spark = SparkSession.builder()
                .config(conf)
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.adaptive.enabled","false")
                //.config("spark.sql.shuffle.partitions", "4")
                //.config("spark.eventLog.dir","file:/tmp/spark-events")
                //.config("spark.eventLog.enabled","true")
                .getOrCreate();
        initializeHadoopCatalog("hadoop", warehouseLocation);
        //System.out.println(spark.conf().get("spark.eventLog.dir"));
    }

    public static void main(String[] args) throws InterruptedException {
        new PopulateData(args).run();
    }

    /**
     * We can compose different sequence of operations in this method to observe the behaviour.
     */
    private void run() throws InterruptedException {
        Catalog catalog = getHadoopCatalog(warehouseLocation);
        Table table = createTable(catalog, "profile", "opportunity");
        System.out.println("------------------\n------------------\n------------------\n------------------\n");
        insertData(table, "device_data"); // seq=1
        System.out.println("------------------\n------------------\n------------------\n------------------\n");
        markTableForRelativePath(table);

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

    private Table createTable(Catalog catalog, String database, String tableName) {
        if(catalog.tableExists(TableIdentifier.of(database, tableName))){
            dropTable(catalog, database, tableName);
        }

        List a = new ArrayList<Types.NestedField>();
                a.add(required(0, "device_id", Types.IntegerType.get()));
                a.add(optional(1, "device_name", Types.StringType.get()));

        Set set = new HashSet<Integer>();
        set.add(0);
        Schema schema = new Schema(
                a,
                set);
        Map<String, String> properties = new HashMap<>();
        properties.put("format-version", "2");
        properties.put("write.format.default", "parquet");
        properties.put("write.target-file-size-bytes", "134217728");
        properties.put("write.parquet.row-group-size-bytes", "134217728");

        return
                catalog.
                        buildTable(TableIdentifier.of(database, tableName), schema).
                        withSortOrder(SortOrder.builderFor(schema).asc("device_id", NullOrder.NULLS_FIRST).build()).
                        withProperties(properties).
                        withPartitionSpec(PartitionSpec.builderFor(schema).bucket("device_id", bucketSize).build()).
                        //withProperty("write.distribution-mode", "hash").
                                create();
    }

    private void dropTable(Catalog catalog, String database, String tableName){
        catalog.dropTable(TableIdentifier.of(database, tableName));
    }

    private void insertData(Table table, String fileName){
        String inputDataLocation = "file://"+System.getProperty("user.dir")+"/src/main/resources/warehouse/input/"+fileName+".csv";
        IcebergSpark.registerBucketUDF(spark, "iceberg_bucket", DataTypes.IntegerType, bucketSize);

        Dataset<Row> inputData = spark.read().option("header", true).csv(inputDataLocation).
                withColumn("device_id", col("device_id").cast("integer")).
                withColumn("device_id", new Column(new AssertNotNull(col("device_id").expr(), scala.collection.immutable.List.empty())));
        //repartition(expr("iceberg_bucket(device_id)")).
        //sortWithinPartitions(expr("iceberg_bucket(device_id)"));
        inputData.createOrReplaceTempView(fileName);

        //spark.sql("INSERT INTO "+table.toString()+" SELECT * FROM "+fileName);
        try {
            inputData.writeTo(table.toString()).append();
        } catch (NoSuchTableException e) {
            e.printStackTrace();
        }
    }

    private void markTableForRelativePath(Table table) {
        table.updateProperties()
                .set("write.metadata.use.relative-path", "true")
                .set("prefix", "file:/opt/warehouse")
                .commit();
    }
}
