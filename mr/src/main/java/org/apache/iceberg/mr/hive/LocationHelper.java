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

package org.apache.iceberg.mr.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.iceberg.mr.InputFormatConfig;

class LocationHelper {
  private static final String TO_COMMIT_EXTENSION = ".toCommit";

  private LocationHelper() {
  }

  /**
   * Generates query directory location based on the configuration.
   * Currently it uses tableLocation/queryId
   * @param conf The job's configuration
   * @return The directory to store the query result files
   */
  static String generateQueryLocation(Configuration conf) {
    String tableLocation = conf.get(InputFormatConfig.TABLE_LOCATION);
    String queryId = conf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
    return tableLocation + "/" + queryId;
  }

  /**
   * Generates the job temp location based on the job configuration.
   * Currently it uses QUERY_LOCATION/jobId.
   * @param conf The job's configuration
   * @param jobId The JobID for the task
   * @return The file to store the results
   */
  static String generateJobLocation(Configuration conf, JobID jobId) {
    return generateQueryLocation(conf) + "/" + jobId;
  }

  /**
   * Generates datafile location based on the task configuration.
   * Currently it uses QUERY_LOCATION/jobId/taskAttemptId.
   * @param conf The job's configuration
   * @param taskAttemptId The TaskAttemptID for the task
   * @return The file to store the results
   */
  static String generateDataFileLocation(Configuration conf, TaskAttemptID taskAttemptId) {
    return generateJobLocation(conf, taskAttemptId.getJobID()) + "/" + taskAttemptId.toString();
  }

  /**
   * Generates file location based on the task configuration and a specific task id.
   * This file will be used to store the data required to generate the Iceberg commit.
   * Currently it uses QUERY_LOCATION/jobId/task-[0..numTasks].toCommit.
   * @param conf The job's configuration
   * @param jobId The jobId for the task
   * @param taskId The taskId for the commit file
   * @return The file to store the results
   */
  static String generateToCommitFileLocation(Configuration conf, JobID jobId, int taskId) {
    return generateJobLocation(conf, jobId) + "/task-" + taskId + TO_COMMIT_EXTENSION;
  }

  /**
   * Generates file location location based on the task configuration.
   * This file will be used to store the data required to generate the Iceberg commit.
   * Currently it uses QUERY_LOCATION/jobId/task-[0..numTasks].committed.
   * @param conf The job's configuration
   * @param taskAttemptId The TaskAttemptID for the task
   * @return The file to store the results
   */
  static String generateToCommitFileLocation(Configuration conf, TaskAttemptID taskAttemptId) {
    return generateToCommitFileLocation(conf, taskAttemptId.getJobID(), taskAttemptId.getTaskID().getId());
  }
}
