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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

public class TestBaseSparkAction extends SparkTestBase {

  @Test
  public void testCancelSparkActionJob() throws InterruptedException {
    String groupId = "test-group-001";
    AtomicReference<Boolean> result = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    spark
        .sparkContext()
        .addSparkListener(
            new SparkListener() {
              @Override
              public void onJobStart(SparkListenerJobStart jobStart) {
                latch.countDown();
              }
            });
    Thread thread =
        new Thread(
            () -> {
              sparkContext.setJobGroup(groupId, "test-cancel-spark-action-job", true);
              String expectedErrMsg = "cancelled job group " + groupId;
              SleepSparkAction action = new SleepSparkAction(spark, 10000L, expectedErrMsg);
              result.set(action.execute());
            });
    thread.start();
    latch.await();
    sparkContext.cancelJobGroup(groupId);
    thread.join();
    Assert.assertFalse(result.get());
  }

  private class SleepSparkAction extends BaseSparkAction<SleepSparkAction> {

    private Long sleep;
    private String expectedErrMsg;

    protected SleepSparkAction(SparkSession spark, Long sleep, String expectedErrMsg) {
      super(spark);
      this.sleep = sleep;
      this.expectedErrMsg = expectedErrMsg;
    }

    private Boolean execute() {
      JobGroupInfo info = newJobGroupInfo("TEST-SLEEP", "Test Sleep");
      return withJobGroupInfo(info, this::doExecute);
    }

    private Boolean doExecute() {
      try {
        spark().sql("select java_method('java.lang.Thread', 'sleep', " + sleep + "L)").collect();
        return true;
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains(expectedErrMsg));
        return false;
      }
    }

    @Override
    protected SleepSparkAction self() {
      return this;
    }
  }
}
