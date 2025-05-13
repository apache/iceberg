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
package org.apache.iceberg.aws.glue;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

public class GlueProperties {
  /**
   * The ID of the Glue Data Catalog where the tables reside. If none is provided, Glue
   * automatically uses the caller's AWS account ID by default.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html
   */
  public static final String GLUE_CATALOG_ID = "glue.id";

  /**
   * The account ID used in a Glue resource ARN, e.g.
   * arn:aws:glue:us-east-1:012345678901:table/db1/table1
   */
  public static final String GLUE_ACCOUNT_ID = "glue.account-id";

  /**
   * If Glue should skip archiving an old table version when creating a new version in a commit. By
   * default, Glue archives all old table versions after an UpdateTable call, but Glue has a default
   * max number of archived table versions (can be increased). So for streaming use case with lots
   * of commits, it is recommended to set this value to true.
   */
  public static final String GLUE_CATALOG_SKIP_ARCHIVE = "glue.skip-archive";

  public static final boolean GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT = true;

  /**
   * If Glue should skip name validations, it is recommended to stick to Glue best practice in
   * https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html to make sure operations
   * are Hive compatible. This is only added for users that have existing conventions using
   * non-standard characters. When database name and table name validation are skipped, there is no
   * guarantee that downstream systems would all support the names.
   */
  public static final String GLUE_CATALOG_SKIP_NAME_VALIDATION = "glue.skip-name-validation";

  public static final boolean GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT = false;

  /**
   * If set, GlueCatalog will use Lake Formation for access control. For more credential vending
   * details, see: https://docs.aws.amazon.com/lake-formation/latest/dg/api-overview.html. If
   * enabled, the {@link AwsClientFactory} implementation must be {@link
   * LakeFormationAwsClientFactory} or any class that extends it.
   */
  public static final String GLUE_LAKEFORMATION_ENABLED = "glue.lakeformation-enabled";

  public static final boolean GLUE_LAKEFORMATION_ENABLED_DEFAULT = false;

  /**
   * Configure an alternative endpoint of the Glue service for GlueCatalog to access.
   *
   * <p>This could be used to use GlueCatalog with any glue-compatible metastore service that has a
   * different endpoint
   */
  public static final String GLUE_CATALOG_ENDPOINT = "glue.endpoint";

  private final String glueEndpoint;
  private String glueCatalogId;
  private boolean glueCatalogSkipArchive;
  private boolean glueCatalogSkipNameValidation;
  private boolean glueLakeFormationEnabled;

  public GlueProperties() {
    this.glueEndpoint = null;
    this.glueCatalogId = null;
    this.glueCatalogSkipArchive = GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT;
    this.glueCatalogSkipNameValidation = GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT;
    this.glueLakeFormationEnabled = GLUE_LAKEFORMATION_ENABLED_DEFAULT;
  }

  public GlueProperties(Map<String, String> properties) {
    this.glueEndpoint = properties.get(GLUE_CATALOG_ENDPOINT);
    this.glueCatalogId = properties.get(GLUE_CATALOG_ID);
    this.glueCatalogSkipArchive =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_CATALOG_SKIP_ARCHIVE, GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT);
    this.glueCatalogSkipNameValidation =
        PropertyUtil.propertyAsBoolean(
            properties,
            GLUE_CATALOG_SKIP_NAME_VALIDATION,
            GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT);
    this.glueLakeFormationEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_LAKEFORMATION_ENABLED, GLUE_LAKEFORMATION_ENABLED_DEFAULT);
  }

  public String glueCatalogId() {
    return glueCatalogId;
  }

  public void setGlueCatalogId(String id) {
    this.glueCatalogId = id;
  }

  public boolean glueCatalogSkipArchive() {
    return glueCatalogSkipArchive;
  }

  public void setGlueCatalogSkipArchive(boolean skipArchive) {
    this.glueCatalogSkipArchive = skipArchive;
  }

  public boolean glueCatalogSkipNameValidation() {
    return glueCatalogSkipNameValidation;
  }

  public void setGlueCatalogSkipNameValidation(boolean glueCatalogSkipNameValidation) {
    this.glueCatalogSkipNameValidation = glueCatalogSkipNameValidation;
  }

  public boolean glueLakeFormationEnabled() {
    return glueLakeFormationEnabled;
  }

  public void setGlueLakeFormationEnabled(boolean glueLakeFormationEnabled) {
    this.glueLakeFormationEnabled = glueLakeFormationEnabled;
  }

  /**
   * Override the endpoint for a glue client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     GlueClient.builder().applyMutation(glueProperties::applyEndpointConfigurations)
   * </pre>
   */
  public <T extends GlueClientBuilder> void applyEndpointConfigurations(T builder) {
    if (glueEndpoint != null) {
      builder.endpointOverride(URI.create(glueEndpoint));
    }
  }
}
