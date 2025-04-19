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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

public class S3ExecutionInterceptor implements ExecutionInterceptor {
  private final List<StorageCredential> storageCredentials;

  public S3ExecutionInterceptor(List<StorageCredential> storageCredentials) {
    Preconditions.checkArgument(null != storageCredentials, "Invalid storage credentials: null");
    this.storageCredentials = storageCredentials;
  }

  @Override
  public SdkHttpRequest modifyHttpRequest(
      Context.ModifyHttpRequest context, ExecutionAttributes executionAttributes) {
    if (!storageCredentials.isEmpty()) {
      SdkHttpRequest.Builder builder = context.httpRequest().toBuilder();
      String requestPath = context.httpRequest().encodedPath();
      StorageCredential credentialToUse = storageCredentials.get(0);
      Map<String, String> configToUse = null;

      for (StorageCredential storageCredential : storageCredentials) {
        if (requestPath.startsWith(storageCredential.prefix())
            && storageCredential.prefix().length() >= credentialToUse.prefix().length()) {
          configToUse = storageCredential.config();
        }
      }

      if (null != configToUse) {
        String accessKeyId = configToUse.get(S3FileIOProperties.ACCESS_KEY_ID);
        String secretAccessKey = configToUse.get(S3FileIOProperties.SECRET_ACCESS_KEY);
        String sessionToken = configToUse.get(S3FileIOProperties.SESSION_TOKEN);
        String tokenExpiresAtMillis =
            configToUse.get(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS);

        if (null != accessKeyId && null != secretAccessKey) {
          builder.putHeader("X-Amz-Access-Key", accessKeyId);
          builder.putHeader("X-Amz-Secret-Key", secretAccessKey);
          if (null != sessionToken) {
            builder.putHeader("X-Amz-Security-Token", sessionToken);
          }

          if (null != tokenExpiresAtMillis) {
            Instant expiresAt = Instant.ofEpochMilli(Long.parseLong(tokenExpiresAtMillis));
            long expiresInSeconds =
                Math.max(1, expiresAt.minusMillis(Instant.now().toEpochMilli()).getEpochSecond());
            builder.putHeader("X-Amz-Expires", Long.toString(expiresInSeconds));
          }
        }

        return builder.build();
      }
    }

    return ExecutionInterceptor.super.modifyHttpRequest(context, executionAttributes);
  }
}
