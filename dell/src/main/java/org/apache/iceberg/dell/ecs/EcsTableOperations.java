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
package org.apache.iceberg.dell.ecs;

import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class EcsTableOperations extends BaseMetastoreTableOperations {

  public static final String ICEBERG_METADATA_LOCATION = "iceberg_metadata_location";

  private final String tableName;
  private final FileIO fileIO;
  private final EcsCatalog catalog;
  private final EcsURI tableObject;

  /**
   * Cached E-Tag for CAS commit
   *
   * @see #doRefresh() when reset this field
   * @see #doCommit(TableMetadata, TableMetadata) when use this field
   */
  private String eTag;

  public EcsTableOperations(
      String tableName, EcsURI tableObject, FileIO fileIO, EcsCatalog catalog, int metadataRefreshMaxRetries) {
    setMetadataRefreshMaxRetries(metadataRefreshMaxRetries);
    this.tableName = tableName;
    this.tableObject = tableObject;
    this.fileIO = fileIO;
    this.catalog = catalog;
  }

  @Override
  protected String tableName() {
    return tableName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation;
    if (!catalog.objectMetadata(tableObject).isPresent()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Metadata object %s is absent while refresh a loaded table. "
                + "Maybe the table is deleted/moved.",
            tableObject);
      } else {
        metadataLocation = null;
      }
    } else {
      EcsCatalog.Properties metadata = catalog.loadProperties(tableObject);
      this.eTag = metadata.eTag();
      metadataLocation = metadata.content().get(ICEBERG_METADATA_LOCATION);
      Preconditions.checkNotNull(
          metadataLocation, "Can't find location from table metadata %s", tableObject);
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    if (base == null) {
      // create a new table, the metadataKey should be absent
      if (!catalog.putNewProperties(tableObject, buildProperties(newMetadataLocation))) {
        throw new CommitFailedException("Table is existing when create table %s", tableName());
      }
    } else {
      String cachedETag = eTag;
      Preconditions.checkNotNull(cachedETag, "E-Tag must be not null when update table");
      // replace to a new version, the E-Tag should be present and matched
      boolean result =
          catalog.updatePropertiesObject(
              tableObject, cachedETag, buildProperties(newMetadataLocation));
      if (!result) {
        throw new CommitFailedException(
            "Replace failed, E-Tag %s mismatch for table %s", cachedETag, tableName());
      }
    }
  }

  /** Build properties for table */
  private Map<String, String> buildProperties(String metadataLocation) {
    return ImmutableMap.of(ICEBERG_METADATA_LOCATION, metadataLocation);
  }
}
