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

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.aws.AssumeRoleAwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;
import software.amazon.awssdk.services.lakeformation.model.PermissionType;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.cache.CachedSupplier;
import software.amazon.awssdk.utils.cache.RefreshResult;

/**
 * This implementation of AwsClientFactory is used by default if {@link
 * org.apache.iceberg.aws.AwsProperties#GLUE_LAKEFORMATION_ENABLED} is set to true. It uses the
 * default credential chain to assume role. Third-party engines can further extend this class to any
 * custom credential setup.
 *
 * <p>It extends AssumeRoleAwsClientFactory to reuse the assuming-role approach for all clients
 * except S3 and KMS. If a table is registered with LakeFormation, the S3/KMS client will use
 * LakeFormation vended credentials, otherwise it uses AssumingRole credentials. For using
 * LakeFormation credential vending for a third-party query engine, see:
 * https://docs.aws.amazon.com/lake-formation/latest/dg/register-query-engine.html
 */
public class LakeFormationAwsClientFactory extends AssumeRoleAwsClientFactory {

  public static final String LF_AUTHORIZED_CALLER = "LakeFormationAuthorizedCaller";
  private static final long CREDENTIAL_PREFETCH_SECONDS = 60L;
  private static final ConcurrentMap<CredentialCacheKey, CredentialCacheEntry> CREDENTIAL_CACHES =
      Maps.newConcurrentMap();

  private String dbName;
  private String tableName;
  private String glueCatalogId;
  private String glueAccountId;
  private Map<String, String> catalogProperties;
  private long credentialCacheExpirationMs;

  public LakeFormationAwsClientFactory() {}

  @Override
  public void initialize(Map<String, String> properties) {
    super.initialize(properties);
    this.catalogProperties = ImmutableMap.copyOf(properties);
    Preconditions.checkArgument(
        awsProperties().stsClientAssumeRoleTags().stream()
            .anyMatch(t -> LF_AUTHORIZED_CALLER.equals(t.key())),
        "STS assume role session tag %s must be set using %s to use LakeFormation client factory",
        LF_AUTHORIZED_CALLER,
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX);
    this.dbName = properties.get(AwsProperties.LAKE_FORMATION_DB_NAME);
    this.tableName = properties.get(AwsProperties.LAKE_FORMATION_TABLE_NAME);
    this.glueCatalogId = properties.get(AwsProperties.GLUE_CATALOG_ID);
    this.glueAccountId = properties.get(AwsProperties.GLUE_ACCOUNT_ID);
    long credentialCacheExpirationSeconds =
        PropertyUtil.propertyAsLong(
            properties,
            AwsProperties.LAKE_FORMATION_CREDENTIAL_CACHE_EXPIRATION_SECONDS,
            AwsProperties.LAKE_FORMATION_CREDENTIAL_CACHE_EXPIRATION_SECONDS_DEFAULT);
    Preconditions.checkArgument(
        credentialCacheExpirationSeconds > 0,
        "Invalid Lake Formation credential cache expiration: %s",
        credentialCacheExpirationSeconds);
    this.credentialCacheExpirationMs = TimeUnit.SECONDS.toMillis(credentialCacheExpirationSeconds);
  }

  @Override
  public S3Client s3() {
    if (isTableRegisteredWithLakeFormation()) {
      return S3Client.builder()
          .applyMutation(awsClientProperties()::applyLegacyMd5Plugin)
          .applyMutation(httpClientProperties()::applyHttpClientConfigurations)
          .applyMutation(s3FileIOProperties()::applyEndpointConfigurations)
          .applyMutation(s3FileIOProperties()::applyServiceConfigurations)
          .applyMutation(s3FileIOProperties()::applyRetryConfigurations)
          .credentialsProvider(lakeFormationCredentialsProvider())
          .region(Region.of(region()))
          .build();
    } else {
      return super.s3();
    }
  }

  @Override
  public KmsClient kms() {
    if (isTableRegisteredWithLakeFormation()) {
      return KmsClient.builder()
          .applyMutation(httpClientProperties()::applyHttpClientConfigurations)
          .applyMutation(awsClientProperties()::applyRetryConfigurations)
          .credentialsProvider(lakeFormationCredentialsProvider())
          .region(Region.of(region()))
          .build();
    } else {
      return super.kms();
    }
  }

  private boolean isTableRegisteredWithLakeFormation() {
    Preconditions.checkArgument(
        dbName != null && !dbName.isEmpty(), "Database name can not be empty");
    Preconditions.checkArgument(
        tableName != null && !tableName.isEmpty(), "Table name can not be empty");

    GetTableResponse response =
        glue()
            .getTable(
                GetTableRequest.builder()
                    .catalogId(glueCatalogId)
                    .databaseName(dbName)
                    .name(tableName)
                    .build());
    return response.table().isRegisteredWithLakeFormation();
  }

  protected String buildTableArn() {
    Preconditions.checkArgument(
        glueAccountId != null && !glueAccountId.isEmpty(),
        "%s can not be empty",
        AwsProperties.GLUE_ACCOUNT_ID);
    String partitionName = PartitionMetadata.of(Region.of(region())).id();
    return String.format(
        "arn:%s:glue:%s:%s:table/%s/%s", partitionName, region(), glueAccountId, dbName, tableName);
  }

  protected LakeFormationClient lakeFormation() {
    return LakeFormationClient.builder()
        .applyMutation(this::applyAssumeRoleConfigurations)
        .applyMutation(httpClientProperties()::applyHttpClientConfigurations)
        .build();
  }

  /**
   * Returns the provider used by S3 and KMS clients for tables registered with Lake Formation.
   * Subclasses can override this method to use {@link #directLakeFormationCredentialsProvider()} or
   * another provider with a different caching policy.
   */
  protected AwsCredentialsProvider lakeFormationCredentialsProvider() {
    return cachedCredentialsProvider(
        buildTableArn(), catalogProperties, credentialCacheExpirationMs, this::lakeFormation);
  }

  /** Returns a provider that requests Lake Formation credentials for every resolution. */
  protected AwsCredentialsProvider directLakeFormationCredentialsProvider() {
    return new LakeFormationCredentialsProvider(lakeFormation(), buildTableArn());
  }

  @VisibleForTesting
  static void clearCredentialCaches() {
    CREDENTIAL_CACHES.values().forEach(CredentialCacheEntry::close);
    CREDENTIAL_CACHES.clear();
  }

  @VisibleForTesting
  static AwsCredentialsProvider cachedCredentialsProvider(
      String tableArn,
      Map<String, String> properties,
      long idleTimeoutMs,
      Supplier<LakeFormationClient> clientSupplier) {
    return new CachedLakeFormationCredentialsProvider(
        new CredentialCacheKey(tableArn, ImmutableMap.copyOf(properties)),
        idleTimeoutMs,
        clientSupplier);
  }

  static class LakeFormationCredentialsProvider implements AwsCredentialsProvider {
    private final LakeFormationClient client;
    private final String tableArn;

    LakeFormationCredentialsProvider(LakeFormationClient lakeFormationClient, String tableArn) {
      this.client = lakeFormationClient;
      this.tableArn = tableArn;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return refreshCredentials().value();
    }

    RefreshResult<AwsCredentials> refreshCredentials() {
      GetTemporaryGlueTableCredentialsResponse response =
          client.getTemporaryGlueTableCredentials(
              GetTemporaryGlueTableCredentialsRequest.builder()
                  .tableArn(tableArn)
                  // Now only two permission types (COLUMN_PERMISSION and CELL_FILTER_PERMISSION)
                  // are supported and Iceberg only supports COLUMN_PERMISSION at this time
                  .supportedPermissionTypes(PermissionType.COLUMN_PERMISSION)
                  .build());
      Instant expiration = response.expiration();
      AwsCredentials credentials =
          AwsSessionCredentials.builder()
              .accessKeyId(response.accessKeyId())
              .secretAccessKey(response.secretAccessKey())
              .sessionToken(response.sessionToken())
              .expirationTime(expiration)
              .build();
      return RefreshResult.builder(credentials)
          .staleTime(expiration)
          .prefetchTime(expiration.minusSeconds(CREDENTIAL_PREFETCH_SECONDS))
          .build();
    }
  }

  private static class CachedLakeFormationCredentialsProvider implements AwsCredentialsProvider {
    private final CredentialCacheKey cacheKey;
    private final long idleTimeoutMs;
    private final Supplier<LakeFormationClient> clientSupplier;

    private CachedLakeFormationCredentialsProvider(
        CredentialCacheKey cacheKey,
        long idleTimeoutMs,
        Supplier<LakeFormationClient> clientSupplier) {
      this.cacheKey = cacheKey;
      this.idleTimeoutMs = idleTimeoutMs;
      this.clientSupplier = clientSupplier;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      long now = System.currentTimeMillis();
      evictIdleCredentialCaches(now);
      CredentialCacheEntry entry =
          CREDENTIAL_CACHES.compute(
              cacheKey,
              (ignored, existing) -> {
                if (existing == null || existing.isIdle(now, idleTimeoutMs)) {
                  if (existing != null) {
                    existing.close();
                  }

                  return new CredentialCacheEntry(
                      clientSupplier.get(), cacheKey.tableArn, idleTimeoutMs, now);
                }

                existing.access(now);
                return existing;
              });
      return entry.resolveCredentials();
    }
  }

  private static void evictIdleCredentialCaches(long now) {
    CREDENTIAL_CACHES.forEach(
        (cacheKey, entry) -> {
          if (entry.isIdle(now) && CREDENTIAL_CACHES.remove(cacheKey, entry)) {
            entry.close();
          }
        });
  }

  private static class CredentialCacheEntry {
    private final LakeFormationClient client;
    private final CachedSupplier<AwsCredentials> credentialCache;
    private final long idleTimeoutMs;
    private long lastAccessTimeMs;

    private CredentialCacheEntry(
        LakeFormationClient client, String tableArn, long idleTimeoutMs, long now) {
      this.client = client;
      this.credentialCache =
          CachedSupplier.builder(
                  new LakeFormationCredentialsProvider(client, tableArn)::refreshCredentials)
              .cachedValueName(LakeFormationCredentialsProvider.class.getName())
              .build();
      this.idleTimeoutMs = idleTimeoutMs;
      this.lastAccessTimeMs = now;
    }

    private synchronized AwsCredentials resolveCredentials() {
      this.lastAccessTimeMs = System.currentTimeMillis();
      return credentialCache.get();
    }

    private synchronized void access(long now) {
      this.lastAccessTimeMs = now;
    }

    private synchronized boolean isIdle(long now) {
      return isIdle(now, idleTimeoutMs);
    }

    private synchronized boolean isIdle(long now, long timeoutMs) {
      return now - lastAccessTimeMs >= timeoutMs;
    }

    private synchronized void close() {
      credentialCache.close();
      client.close();
    }
  }

  private static class CredentialCacheKey {
    private final String tableArn;
    private final Map<String, String> properties;

    private CredentialCacheKey(String tableArn, Map<String, String> properties) {
      this.tableArn = tableArn;
      this.properties = properties;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (other == null || getClass() != other.getClass()) {
        return false;
      }

      CredentialCacheKey that = (CredentialCacheKey) other;
      return tableArn.equals(that.tableArn) && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
      return 31 * tableArn.hashCode() + properties.hashCode();
    }
  }
}
