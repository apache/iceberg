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
package org.apache.iceberg.aws.s3;

import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.s3accessgrants.plugin.S3AccessGrantsPlugin;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

class S3AccessGrantsPluginConfigurations {
  private boolean isS3AccessGrantsFallbackToIAMEnabled;

  private S3AccessGrantsPluginConfigurations() {}

  public <T extends S3ClientBuilder> void configureS3ClientBuilder(T builder) {
    S3AccessGrantsPlugin s3AccessGrantsPlugin =
        S3AccessGrantsPlugin.builder().enableFallback(isS3AccessGrantsFallbackToIAMEnabled).build();
    builder.addPlugin(s3AccessGrantsPlugin);
  }

  private void initialize(Map<String, String> properties) {
    this.isS3AccessGrantsFallbackToIAMEnabled =
        PropertyUtil.propertyAsBoolean(
            properties,
            S3FileIOProperties.S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED,
            S3FileIOProperties.S3_ACCESS_GRANTS_FALLBACK_TO_IAM_ENABLED_DEFAULT);
  }

  public static S3AccessGrantsPluginConfigurations create(Map<String, String> properties) {
    S3AccessGrantsPluginConfigurations configurations = new S3AccessGrantsPluginConfigurations();
    configurations.initialize(properties);
    return configurations;
  }
}
