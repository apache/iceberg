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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.iceberg.mr.InputFormatConfig;

class LocationHelper {
  private static final String FOR_COMMIT_EXTENSION = ".forCommit";

  private LocationHelper() {
  }

  /**
   * Generates the job temp location based on the job configuration.
   * Currently it uses QUERY_LOCATION-jobId.
   * @param conf The job's configuration
   * @param jobId The JobID for the task
   * @return The file to store the results
   */
  static String generateJobLocation(Configuration conf, JobID jobId) {
    String tableLocation = conf.get(InputFormatConfig.TABLE_LOCATION);
    String queryId = conf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
    return tableLocation + "/temp/" + queryId + "-" + jobId;
  }

  /**
   * Generates file location based on the task configuration and a specific task id.
   * This file will be used to store the data required to generate the Iceberg commit.
   * Currently it uses QUERY_LOCATION-jobId/task-[0..numTasks).forCommit.
   * @param conf The job's configuration
   * @param jobId The jobId for the task
   * @param taskId The taskId for the commit file
   * @return The file to store the results
   */
  static String generateFileForCommitLocation(Configuration conf, JobID jobId, int taskId) {
    return generateJobLocation(conf, jobId) + "/task-" + taskId + FOR_COMMIT_EXTENSION;
  }
}
