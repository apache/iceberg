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
package org.apache.iceberg.gcp.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = TestGcsFileIOWithAppCreds.BUCKET_ENV, matches = ".+"),
  @EnabledIfEnvironmentVariable(
      named = TestGcsFileIOWithAppCreds.GOOGLE_APPLICATION_CREDENTIALS,
      matches = ".*")
})
public class TestGcsFileIOWithAppCreds extends TestGcsFileIO {

  static final String BUCKET_ENV = "GCS_TEST_BUCKET";
  static final String GOOGLE_APPLICATION_CREDENTIALS = "GOOGLE_APPLICATION_CREDENTIALS";

  @Override
  protected void init() throws IOException {
    StorageOptions.Builder builder =
        StorageOptions.newBuilder().setCredentials(GoogleCredentials.getApplicationDefault());
    String projectId = ServiceOptions.getDefaultProjectId();
    if (projectId != null) {
      builder.setProjectId(projectId);
    }

    this.storage = builder.build().getService();
    this.bucket = System.getenv(BUCKET_ENV);
  }
}
