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
package org.apache.iceberg.spark.actions;

import java.net.InetAddress;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.actions.TestRemoveDanglingDeleteFilesAction;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestRemoveDanglingDeleteAction extends TestRemoveDanglingDeleteFilesAction {

  private static SparkSession spark = null;

  @BeforeAll
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config(TestBase.DISABLE_UI)
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    if (spark != null) {
      try {
        spark.stop();
      } finally {
        spark = null;
      }
    }
  }

  @Override
  protected RemoveDanglingDeleteFiles removeDanglingDeleteFiles() {
    return new RemoveDanglingDeletesSparkAction(spark, table);
  }
}
