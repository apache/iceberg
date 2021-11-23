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

package org.apache.iceberg.spark;


import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class test {

    public SparkSession Spark(){

        System.setProperty("HADOOP_USER_NAME", "hdp_teu_dpd");
        Configuration conf = new Configuration();
        conf.addResource("test/core-site.xml");
        conf.addResource("test/hdfs-site.xml");
        conf.addResource("test/mountTable.xml");
        SparkSession spark = SparkSession.builder()
                .appName("test-merge")
                .config("spark.sql.catalog.iceberg_hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.iceberg_hadoop_catalog", SparkCatalog.class.getName())
                /*.config("spark.sql.catalog.dataplat_hadoop_catalog.uri", "thrift://10.162.12.69:9083")*/
                //      .config("spark.sql.catalog.dataplat_hadoop_catalog.warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/warehouse/hive_catalog")
                .config("spark.sql.catalog.iceberg_hadoop_catalog.warehouse", "hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/hadoop_catalog")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                //.enableHiveSupport()
                .master("local[*]")
                .getOrCreate();
        return spark;

    }

    @Test
    public void test(){
        SparkSession spark = Spark();

        spark.sql("use iceberg_hadoop_catalog");
        spark.sql("use hdp_teu_dpd_default_stream_db");
        spark.sql("explain merge into merge_test003 t using (select * from merge_test004) s on t.id = s.id " +
                "when matched then update set t.data = s.data, t.data2 = s.data2 when not matched then insert *").show();
        spark.sql("merge into merge_test003 t using (select * from merge_test004) s on t.id = s.id when matched then update set t.data = s.data, t.data2 = s.data2 when not matched then insert *;");
    }

}
