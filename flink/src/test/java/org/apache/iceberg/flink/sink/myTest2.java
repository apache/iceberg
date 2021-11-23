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

package org.apache.iceberg.flink.sink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.Before;
import org.junit.Test;


public class myTest2 {

    StreamExecutionEnvironment env = null;
    StreamTableEnvironment tenv = null;
    String database = "hdp_teu_dpd_default_stream_db";

    @Before
    public void before(){
        System.setProperty("HADOOP_USER_NAME", "hdp_teu_dpd");
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //flink写入iceberg需要打开checkpoint
        env.enableCheckpointing(20000);
        tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create CATALOG iceberg_hadoop_catalog with" +
                "('type'='iceberg','catalog-type'='hadoop'," +
                "'warehouse'='hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/hadoop_catalog')");
        tenv.useCatalog("iceberg_hadoop_catalog");
        tenv.useDatabase("hdp_teu_dpd_default_stream_db");
        org.apache.flink.configuration.Configuration configuration = tenv.getConfig()
                .getConfiguration();
        configuration.setString("execution.type", "streaming");
        configuration.setString("table.dynamic-table-options.enabled", "true");
        /*tenv.executeSql("SET execution.type = streaming");
        tenv.executeSql("SET table.dynamic-table-options.enabled=true");*/
    }
    public HadoopCatalog getCatalog(){
        HashMap<String, String> config = new HashMap<>();
        config.put("type", "iceberg");
        config.put("catalog-type", "hadoop");
        config.put("property-version", "1");
        config.put("warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/hadoop_catalog");
        HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration());
        hadoopCatalog.initialize("iceberg_hadoop_catalog", config);
        return hadoopCatalog;
    }


    @Test
    public void AlterTable(){
        HadoopCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_00t9"));
        table.updateProperties()
                .set("write.format.default", "avro")
                .set("format-version", "2")
                .set("flink.max-continuous-empty-commits", "50")
                .set("write.upsert.enable", "true").commit();
    }

    @Test
    public void alterSQL(){
        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_alter(\n" +
                "  `id`  INT NOT NULL,\n" +
                "  `data1`   CHAR(255) NOT NULL,\n" +
                "  `data2`   CHAR(255) NOT NULL\n" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true',\n" +
                "'write.metadata.previous-versions-max'='5')");
        tenv.executeSql("ALTER TABLE iceberg_alter SET('flink.max-continuous-empty-commits'='50')");
    }


    @Test
    public void apiReading() throws Exception {
        HadoopCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_cdc3"));
        System.out.println("---------------- " + table.location());
        TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());

        List<Long> snapshots = SnapshotUtil.currentAncestors(table);
        TableSchema projectedSchema = TableSchema.builder()
                .field("id", DataTypes.INT())
                //.field("data1", DataTypes.STRING())
                .field("data2", DataTypes.STRING()).build();

        System.out.println(table.currentSnapshot().snapshotId()+" ~~ " +snapshots);

        DataStream<RowData> dataDataStream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(false)
                .project(projectedSchema)

                .build();
        //dataDataStream.print();
        dataDataStream.map(next ->
                next.getInt(0)+" "+next.getString(1)
        ).writeAsText("/Users/a58/flink-1.13.3/bin/test/no.txt", FileSystem.WriteMode.OVERWRITE);

        //只采用start则会是start --- current
        DataStream<RowData> dataDataStream2 = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(false)
                //.endSnapshotId(3537754291432780882L)
                .startSnapshotId(5344964081468670205L)

                .project(projectedSchema)

                .build();
        dataDataStream2.map(next ->
            next.getInt(0)+" "+next.getString(1)
        ).writeAsText("/Users/a58/flink-1.13.3/bin/test/new.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute("something interesting");

    }

    @Test
    public void testIncrementUpsert() throws Exception {

        /*tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_cdc0(\n" +
                "  `id`  INT NOT NULL,\n" +
                "  `data1`   CHAR(255) NOT NULL,\n" +
                "  `data2`   CHAR(255) NOT NULL,\n" +
                "  PRIMARY KEY(id) NOT ENFORCED\n" +
                ") with('write.format.default'='avro','format-version' = '2','write.upsert.enable'='true','write.metadata.delete-after-commit.enabled'='true',\n" +
                "'write.metadata.previous-versions-max'='5')");*/
        HadoopCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_cdc0"));
        //tenv.executeSql("insert into iceberg_cdc4 values(1,'2','3'),(2,'3','4')");
        //long startId = table.currentSnapshot().snapshotId();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        //[2442615627949428082, 4007692542444171162, 5178418584911757345, 7400664529855690494, 5692288945516625943, 4390957026856107317,
        // 2026720933875655765, 1532913898531337759, 2069252303766301296, 2534615831846106507, 8760631681729080192, 2829398766850348546]
        System.out.println(ancestors);
        //tenv.executeSql("insert into iceberg_cdc4 values(5,'2','3'),(6,'3','4')");
        TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
        FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                //.endSnapshotId(3537754291432780882L)
                .build()
                .print("increment");
        env.execute("something interesting");
    }

    @Test
    public void testIncrementBatch() throws Exception {
        org.apache.flink.configuration.Configuration configuration = tenv.getConfig()
                .getConfiguration();
        configuration.setString("execution.type", "batch");
        configuration.setString("table.dynamic-table-options.enabled", "true");
        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_batch(\n" +
                "  `id`  INT NOT NULL,\n" +
                "  `data1`   CHAR(255) NOT NULL,\n" +
                "  `data2`   CHAR(255) NOT NULL,\n" +
                "  PRIMARY KEY(id) NOT ENFORCED\n" +
                ") with('write.format.default'='avro','format-version' = '2','write.metadata.delete-after-commit.enabled'='true',\n" +
                "'write.metadata.previous-versions-max'='5')");
        HadoopCatalog catalog = getCatalog();
        Table table = catalog.loadTable(TableIdentifier.of(database, "iceberg_batch"));
        /*tenv.executeSql("insert into" +
                " iceberg_batch values(1,'2','3'),(2,'3','4')");*/
        //long startId = table.currentSnapshot().snapshotId();
        List<Long> ancestors = SnapshotUtil.currentAncestors(table);
        //[2442615627949428082, 4007692542444171162, 5178418584911757345, 7400664529855690494, 5692288945516625943, 4390957026856107317,
        // 2026720933875655765, 1532913898531337759, 2069252303766301296, 2534615831846106507, 8760631681729080192, 2829398766850348546]
        System.out.println(ancestors);
//        tenv.executeSql("insert into iceberg_batch values(10,'2','3'),(600,'3','4')");
        TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
        FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .startSnapshotId(8117777596715128690L)
                .build()
                .print("increment");
        env.execute("something interesting");
    }

    @Test
    public void sql(){

        //[8117777596715128690, 4070551942305679938, 7618084359669719904, 1965010231665989137]
        tenv.executeSql(
                "SELECT id,data1,data2 FROM iceberg_cdc0 /*+ OPTIONS('streaming'='true', 'monitor-interval'='1s','start-snapshot-id'='386450742086675880')*/").print();

    }



}
