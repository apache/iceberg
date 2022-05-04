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
package org.apache.iceberg.aws.lakeformation;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

class CachingLakeFormationCredentialsProvider implements AwsCredentialsProvider {
  private final LakeFormationCredentialsCache cache;
  private final String tableArn;
  private final LakeFormationIdentity identity;

  CachingLakeFormationCredentialsProvider(
      String tableArn,
      LakeFormationIdentity identity,
      int maxCacheSize,
      long cacheExpirationInMillis,
      long cacheExpiryLeadTimeInMillis) {
    cache =
        LakeFormationCredentialsCacheFactory.getInstance(maxCacheSize, cacheExpirationInMillis)
            .buildCache(identity, cacheExpiryLeadTimeInMillis);
    this.tableArn = tableArn;
    this.identity = identity;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    LakeFormationTemporaryCredentials credentials = cache.get(tableArn);

    return credentials.credentials();
  }
}
