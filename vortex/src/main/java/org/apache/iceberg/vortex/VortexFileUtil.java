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
package org.apache.iceberg.vortex;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared utilities for resolving file URIs and cloud credentials for Vortex file access. */
final class VortexFileUtil {
  private static final Logger LOG = LoggerFactory.getLogger(VortexFileUtil.class);

  private static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
  private static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
  private static final String FS_S3A_SESSION_TOKEN = "fs.s3a.session.token";
  private static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
  private static final String FS_S3A_ENDPOINT_REGION = "fs.s3a.endpoint.region";
  private static final String ACCESS_KEY_PREFIX = "fs.azure.account.key";
  private static final String FIXED_TOKEN_PREFIX = "fs.azure.sas.fixed.token.";

  private VortexFileUtil() {}

  static String resolveUri(String location) {
    URI uri = URI.create(location);
    if (uri.getScheme() == null) {
      return Paths.get(location).toAbsolutePath().toUri().toString();
    }
    return uri.toString();
  }

  static Map<String, String> resolveOutputProperties(OutputFile outputFile) {
    URI uri = URI.create(outputFile.location());
    if (uri.getScheme() == null) {
      return Map.of();
    }

    if (outputFile instanceof HadoopOutputFile) {
      return resolvePropertiesFromConf(uri, ((HadoopOutputFile) outputFile).getConf());
    }

    return Map.of();
  }

  static Map<String, String> resolveInputProperties(InputFile inputFile) {
    URI uri = URI.create(inputFile.location());
    if (uri.getScheme() == null) {
      return Map.of();
    }

    if (inputFile instanceof HadoopInputFile) {
      return resolvePropertiesFromConf(uri, ((HadoopInputFile) inputFile).getConf());
    }

    return Map.of();
  }

  private static Map<String, String> resolvePropertiesFromConf(URI uri, Configuration conf) {
    Preconditions.checkNotNull(conf, "Hadoop Configuration is required");
    return switch (uri.getScheme()) {
      case "s3a" -> s3PropertiesFromHadoopConf(conf);
      case "wasb", "wasbs", "abfs", "abfss" -> azurePropertiesFromHadoopConf(conf);
      case "file" -> Map.of();
      default -> throw new IllegalArgumentException("Unsupported scheme: " + uri.getScheme());
    };
  }

  private static Map<String, String> s3PropertiesFromHadoopConf(Configuration hadoopConf) {
    VortexS3Properties properties = new VortexS3Properties();
    for (Map.Entry<String, String> entry : hadoopConf) {
      switch (entry.getKey()) {
        case FS_S3A_ACCESS_KEY:
          properties.setAccessKeyId(entry.getValue());
          break;
        case FS_S3A_SECRET_KEY:
          properties.setSecretAccessKey(entry.getValue());
          break;
        case FS_S3A_SESSION_TOKEN:
          properties.setSessionToken(entry.getValue());
          break;
        case FS_S3A_ENDPOINT:
          String qualified = entry.getValue();
          if (!qualified.startsWith("http")) {
            qualified = "https://" + qualified;
          }

          properties.setEndpoint(qualified);
          break;
        case FS_S3A_ENDPOINT_REGION:
          properties.setRegion(entry.getValue());
          break;
        default:
          LOG.trace(
              "Ignoring unknown s3a connector property: {}={}", entry.getKey(), entry.getValue());
          break;
      }
    }

    return properties.asProperties();
  }

  private static Map<String, String> azurePropertiesFromHadoopConf(Configuration hadoopConf) {
    VortexAzureProperties properties = new VortexAzureProperties();
    for (Map.Entry<String, String> entry : hadoopConf) {
      String configKey = entry.getKey();
      if (configKey.startsWith(ACCESS_KEY_PREFIX)) {
        properties.setAccessKey(entry.getValue());
      } else if (configKey.startsWith(FIXED_TOKEN_PREFIX)) {
        properties.setSasKey(entry.getValue());
      } else {
        LOG.trace("Ignoring unknown azure connector property: {}={}", configKey, entry.getValue());
      }
    }

    return properties.asProperties();
  }
}
