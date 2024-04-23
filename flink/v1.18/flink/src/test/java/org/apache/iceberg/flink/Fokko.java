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

package org.apache.iceberg.flink;

public class Fokko extends FlinkTestBase {
    @Test
    public void testFokko() {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE CATALOG tabular WITH (\n" +
                "    'type'='iceberg',\n" +
                "    'catalog-type'='rest',\n" +
                "    'uri'='https://api.tabular.io/ws',\n" +
                "    'credential'='t-yxgj-RqdCOk:A3HESMNI8kHBoDn0Ce4d262icNw',\n" +
                "    'warehouse'='Fokko'\n" +
                ")");

        tEnv.executeSql("USE CATALOG tabular").print();
        tEnv.executeSql("SELECT * FROM examples.nyc_taxi_yellow").print();
    }
}