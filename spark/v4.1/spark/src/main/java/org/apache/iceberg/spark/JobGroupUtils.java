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
package org.apache.iceberg.spark;

import java.util.function.Supplier;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.api.java.JavaSparkContext;

public class JobGroupUtils {

  private static final String JOB_GROUP_ID = SparkContext$.MODULE$.SPARK_JOB_GROUP_ID();
  private static final String JOB_GROUP_DESC = SparkContext$.MODULE$.SPARK_JOB_DESCRIPTION();
  private static final String JOB_INTERRUPT_ON_CANCEL =
      SparkContext$.MODULE$.SPARK_JOB_INTERRUPT_ON_CANCEL();

  private JobGroupUtils() {}

  public static JobGroupInfo getJobGroupInfo(SparkContext sparkContext) {
    String groupId = sparkContext.getLocalProperty(JOB_GROUP_ID);
    String description = sparkContext.getLocalProperty(JOB_GROUP_DESC);
    String interruptOnCancel = sparkContext.getLocalProperty(JOB_INTERRUPT_ON_CANCEL);
    return new JobGroupInfo(groupId, description, Boolean.parseBoolean(interruptOnCancel));
  }

  public static void setJobGroupInfo(SparkContext sparkContext, JobGroupInfo info) {
    sparkContext.setLocalProperty(JOB_GROUP_ID, info.groupId());
    sparkContext.setLocalProperty(JOB_GROUP_DESC, info.description());
    sparkContext.setLocalProperty(
        JOB_INTERRUPT_ON_CANCEL, String.valueOf(info.interruptOnCancel()));
  }

  public static <T> T withJobGroupInfo(
      JavaSparkContext sparkContext, JobGroupInfo info, Supplier<T> supplier) {
    return withJobGroupInfo(sparkContext.sc(), info, supplier);
  }

  public static <T> T withJobGroupInfo(
      SparkContext sparkContext, JobGroupInfo info, Supplier<T> supplier) {
    JobGroupInfo previousInfo = getJobGroupInfo(sparkContext);
    try {
      setJobGroupInfo(sparkContext, info);
      return supplier.get();
    } finally {
      setJobGroupInfo(sparkContext, previousInfo);
    }
  }
}
