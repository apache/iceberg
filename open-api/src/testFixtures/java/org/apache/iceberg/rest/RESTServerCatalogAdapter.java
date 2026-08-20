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
package org.apache.iceberg.rest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.ImmutableFieldLabels;
import org.apache.iceberg.ImmutableLabels;
import org.apache.iceberg.Labels;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTCatalogServer.CatalogContext;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadViewResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.util.PropertyUtil;

class RESTServerCatalogAdapter extends RESTCatalogAdapter {
  private static final String INCLUDE_CREDENTIALS = "include-credentials";
  private static final String INCLUDE_LABELS = "include-labels";

  private static final String CATALOG_NAME_LABEL = "catalog-name";
  private static final String CATALOG_HOST_LABEL = "catalog-host";
  private static final String CLASSIFICATION_LABEL = "classification";

  private static final String HOSTNAME = hostname();

  private final CatalogContext catalogContext;

  RESTServerCatalogAdapter(CatalogContext catalogContext) {
    super(catalogContext.catalog());
    this.catalogContext = catalogContext;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends RESTResponse> T handleRequest(
      Route route,
      Map<String, String> vars,
      HTTPRequest httpRequest,
      Class<T> responseType,
      Consumer<Map<String, String>> responseHeaders) {
    T restResponse = super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);

    if (PropertyUtil.propertyAsBoolean(
        catalogContext.configuration(), INCLUDE_CREDENTIALS, false)) {
      if (restResponse instanceof LoadTableResponse response) {
        applyCredentials(catalogContext.configuration(), response.config());
      } else if (restResponse instanceof PlanTableScanResponse response
          && PlanStatus.COMPLETED == response.planStatus()) {
        return (T)
            PlanTableScanResponse.builder()
                .withPlanStatus(response.planStatus())
                .withPlanId(response.planId())
                .withFileScanTasks(response.fileScanTasks())
                .withSpecsById(response.specsById())
                .withCredentials(createStorageCredentials(catalogContext.configuration()))
                .build();
      } else if (restResponse instanceof FetchPlanningResultResponse response
          && PlanStatus.COMPLETED == response.planStatus()) {
        return (T)
            FetchPlanningResultResponse.builder()
                .withPlanStatus(response.planStatus())
                .withFileScanTasks(response.fileScanTasks())
                .withPlanTasks(response.planTasks())
                .withSpecsById(response.specsById())
                .withCredentials(createStorageCredentials(catalogContext.configuration()))
                .build();
      }
    }

    if (PropertyUtil.propertyAsBoolean(catalogContext.configuration(), INCLUDE_LABELS, false)) {
      if (restResponse instanceof LoadTableResponse response) {
        // rebuild because labels can only be set through the builder. this preserves the metadata
        // as it was produced by the handler, including any snapshot suppression already applied.
        return (T)
            LoadTableResponse.builder()
                .withTableMetadata(response.tableMetadata())
                .addAllConfig(response.config())
                .addAllCredentials(response.credentials())
                .withLabels(labels(response.tableMetadata().schema()))
                .build();
      } else if (restResponse instanceof LoadViewResponse response) {
        return (T)
            ImmutableLoadViewResponse.builder()
                .from(response)
                .labels(labels(response.metadata().schema()))
                .build();
      }
    }

    return restResponse;
  }

  /**
   * Produces labels derived from the catalog and the server environment.
   *
   * <p>These values carry no meaning beyond exercising the labels read path end-to-end: they let
   * clients verify that object-level and field-level labels survive a round trip over the wire.
   */
  private Labels labels(Schema schema) {
    ImmutableLabels.Builder labels =
        ImmutableLabels.builder()
            .putObjectLabels(
                CATALOG_NAME_LABEL,
                PropertyUtil.propertyAsString(
                    catalogContext.configuration(),
                    RESTCatalogServer.CATALOG_NAME,
                    RESTCatalogServer.CATALOG_NAME_DEFAULT));

    if (null != HOSTNAME) {
      labels.putObjectLabels(CATALOG_HOST_LABEL, HOSTNAME);
    }

    // label the first column so that field-level labels are covered without assuming a schema
    if (!schema.columns().isEmpty()) {
      labels.addFields(
          ImmutableFieldLabels.builder()
              .fieldId(schema.columns().get(0).fieldId())
              .putLabels(CLASSIFICATION_LABEL, "public")
              .build());
    }

    return labels.build();
  }

  private static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      // the hostname is not resolvable in all environments, so it is simply omitted
      return null;
    }
  }

  private void applyCredentials(
      Map<String, String> catalogConfig, Map<String, String> tableConfig) {
    if (catalogConfig.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
      tableConfig.put(
          S3FileIOProperties.ACCESS_KEY_ID, catalogConfig.get(S3FileIOProperties.ACCESS_KEY_ID));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)) {
      tableConfig.put(
          S3FileIOProperties.SECRET_ACCESS_KEY,
          catalogConfig.get(S3FileIOProperties.SECRET_ACCESS_KEY));
    }

    if (catalogConfig.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
      tableConfig.put(
          S3FileIOProperties.SESSION_TOKEN, catalogConfig.get(S3FileIOProperties.SESSION_TOKEN));
    }

    if (catalogConfig.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
      tableConfig.put(
          GCPProperties.GCS_OAUTH2_TOKEN, catalogConfig.get(GCPProperties.GCS_OAUTH2_TOKEN));
    }

    catalogConfig.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)
                    || entry.getKey().startsWith(AzureProperties.ADLS_CONNECTION_STRING_PREFIX))
        .forEach(entry -> tableConfig.put(entry.getKey(), entry.getValue()));
  }

  private List<Credential> createStorageCredentials(Map<String, String> catalogConfig) {
    List<Credential> storageCredentials = Lists.newArrayList();

    if (catalogConfig.containsKey(S3FileIOProperties.ACCESS_KEY_ID)
        && catalogConfig.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)
        && catalogConfig.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
      ImmutableCredential.Builder s3 =
          ImmutableCredential.builder()
              .prefix("s3")
              .putConfig(
                  S3FileIOProperties.ACCESS_KEY_ID,
                  catalogConfig.get(S3FileIOProperties.ACCESS_KEY_ID))
              .putConfig(
                  S3FileIOProperties.SECRET_ACCESS_KEY,
                  catalogConfig.get(S3FileIOProperties.SECRET_ACCESS_KEY))
              .putConfig(
                  S3FileIOProperties.SESSION_TOKEN,
                  catalogConfig.get(S3FileIOProperties.SESSION_TOKEN));
      if (catalogConfig.containsKey(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS)) {
        s3.putConfig(
            S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
            catalogConfig.get(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS));
      }

      storageCredentials.add(s3.build());
    }

    if (catalogConfig.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
      ImmutableCredential.Builder gcs =
          ImmutableCredential.builder()
              .prefix("gcp")
              .putConfig(
                  GCPProperties.GCS_OAUTH2_TOKEN,
                  catalogConfig.get(GCPProperties.GCS_OAUTH2_TOKEN));

      if (catalogConfig.containsKey(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT)) {
        gcs.putConfig(
            GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
            catalogConfig.get(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT));
      }

      storageCredentials.add(gcs.build());
    }

    if (catalogConfig.entrySet().stream()
        .anyMatch(entry -> entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX))) {
      ImmutableCredential.Builder adls = ImmutableCredential.builder().prefix("adls");
      catalogConfig.entrySet().stream()
          .filter(
              entry ->
                  entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)
                      || entry.getKey().startsWith(AzureProperties.ADLS_CONNECTION_STRING_PREFIX)
                      || entry
                          .getKey()
                          .startsWith(AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX))
          .forEach(entry -> adls.putConfig(entry.getKey(), entry.getValue()));
      storageCredentials.add(adls.build());
    }

    return storageCredentials;
  }
}
