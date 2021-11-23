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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.flink.source.FlinkSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class myTest {

    public static void main(String[] args) {


        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //flink写入iceberg需要打开checkpoint
        env.enableCheckpointing(60000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("CREATE TABLE sourceTablep1 (\n" +
                "user_id int,\n" +
                "f_random_str STRING\n" +
                ") WITH \n" +
                "('connector' = 'datagen',\n" +
                "'rows-per-second'='100000',\n" +
                "'fields.user_id.kind'='random',\n" +
                "'fields.user_id.max'='1000',\n" +
                "'fields.f_random_str.length'='10'\n" +
                ")");
        tenv.executeSql("create CATALOG iceberg_hadoop_catalog with" +
                "('type'='iceberg','catalog-type'='hadoop'," +
                "'warehouse'='hdfs://10.162.12.100:9000/home/hdp_teu_dpd/resultdata/ly60/iceberg/hadoop_catalog')");

        tenv.useCatalog("iceberg_hadoop_catalog");
        tenv.useDatabase("hdp_teu_dpd_default_stream_db");

        tenv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_00t9 (\n" +
                "    user_id int COMMENT 'user_id',\n" +
                "    f_random_str STRING COMMENT 'f_random_str'" +
                ")");

        tenv.executeSql(
                "INSERT INTO iceberg_00t9 " +
                        " SELECT user_id, f_random_str FROM default_catalog.default_database.sourceTablep1");
    }
}