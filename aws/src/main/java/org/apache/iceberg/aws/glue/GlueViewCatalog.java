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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.*;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;

public class GlueViewCatalog extends MetastoreViewCatalog
    implements Closeable, Configurable<Configuration> {

  private GlueClient glue;
  private Object hadoopConf;
  private String catalogName;
  private String warehousePath;
  private AwsProperties awsProperties;
  private FileIO fileIO;
  private LockManager lockManager;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;

  private static final DynMethods.UnboundMethod SET_VERSION_ID =
      DynMethods.builder("versionId")
          .hiddenImpl(
              "software.amazon.awssdk.services.glue.model.UpdateTableRequest$Builder", String.class)
          .orNoop()
          .build();

  public GlueViewCatalog(Configuration conf) {
    super(conf);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    AwsClientFactory awsClientFactory;
    FileIO catalogFileIO;
    if (PropertyUtil.propertyAsBoolean(
        properties,
        AwsProperties.GLUE_LAKEFORMATION_ENABLED,
        AwsProperties.GLUE_LAKEFORMATION_ENABLED_DEFAULT)) {
      String factoryImpl =
          PropertyUtil.propertyAsString(properties, AwsProperties.CLIENT_FACTORY, null);
      ImmutableMap.Builder<String, String> builder =
          ImmutableMap.<String, String>builder().putAll(properties);
      if (factoryImpl == null) {
        builder.put(AwsProperties.CLIENT_FACTORY, LakeFormationAwsClientFactory.class.getName());
      }

      this.catalogProperties = builder.build();
      awsClientFactory = AwsClientFactories.from(catalogProperties);
      Preconditions.checkArgument(
          awsClientFactory instanceof LakeFormationAwsClientFactory,
          "Detected LakeFormation enabled for Glue catalog, should use a client factory that extends %s, but found %s",
          LakeFormationAwsClientFactory.class.getName(),
          factoryImpl);
      catalogFileIO = null;
    } else {
      awsClientFactory = AwsClientFactories.from(properties);
      catalogFileIO = initializeFileIO(properties);
    }

    initialize(
        name,
        properties.get(CatalogProperties.WAREHOUSE_LOCATION),
        new AwsProperties(properties),
        awsClientFactory.glue(),
        initializeLockManager(properties),
        catalogFileIO);
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected GlueViewOperations newViewOps(TableIdentifier viewName) {
    return new GlueViewOperations(glue, lockManager, catalogName, awsProperties, fileIO, viewName);
  }

  @Override
  public boolean dropView(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException();
  }

  @VisibleForTesting
  void initialize(
      String name,
      String path,
      AwsProperties properties,
      GlueClient client,
      LockManager lock,
      FileIO io) {
    Preconditions.checkArgument(
        path != null && path.length() > 0,
        "Cannot initialize GlueCatalog because warehousePath must not be null or empty");

    this.catalogName = name;
    this.awsProperties = properties;
    this.warehousePath = LocationUtil.stripTrailingSlash(path);
    this.glue = client;
    this.lockManager = lock;
    this.fileIO = io;

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(glue);
    closeableGroup.addCloseable(lockManager);
    closeableGroup.addCloseable(fileIO);
    closeableGroup.setSuppressCloseFailure(true);
  }

  private LockManager initializeLockManager(Map<String, String> properties) {
    if (properties.containsKey(CatalogProperties.LOCK_IMPL)) {
      return LockManagers.from(properties);
    } else if (SET_VERSION_ID.isNoop()) {
      return LockManagers.defaultLockManager();
    }
    return null;
  }

  private FileIO initializeFileIO(Map<String, String> properties) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      FileIO io = new S3FileIO();
      io.initialize(properties);
      return io;
    } else {
      return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    }
  }

  @Override
  public void close() throws IOException {
    glue.close();
  }

  /**
   * This method produces the same result as using a HiveCatalog. If databaseUri exists for the Glue
   * database URI, the default location is databaseUri/tableName. If not, the default location is
   * warehousePath/databaseName.db/tableName
   *
   * @param tableIdentifier table id
   * @return default warehouse path
   */
  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // check if value is set in database
    GetDatabaseResponse response =
        glue.getDatabase(
            GetDatabaseRequest.builder()
                .name(
                    IcebergToGlueConverter.getDatabaseName(
                        tableIdentifier, awsProperties.glueCatalogSkipNameValidation()))
                .build());
    String dbLocationUri = response.database().locationUri();
    if (dbLocationUri != null) {
      return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
    }

    return String.format(
        "%s/%s.db/%s",
        warehousePath,
        IcebergToGlueConverter.getDatabaseName(
            tableIdentifier, awsProperties.glueCatalogSkipNameValidation()),
        tableIdentifier.name());
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }
}
