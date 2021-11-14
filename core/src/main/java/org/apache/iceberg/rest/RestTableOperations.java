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

import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.http.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO - Extract out to an interface - Implement with HTTP version.
// TODO - Should we implement Configurable here? Since this will be an interface, I think not in the interface.
// TODO - As this will be more of an interface, possibly extend TableOperations directly (like HadoopTableOperations)
class RestTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(RestTableOperations.class);

  private final TableIdentifier tableIdentifier;
  private final String fullTableName;
  // TODO - Use the RestClient interface here instead.
  private final HttpClient httpClient;
  private final Map<String, String> properties;
  private final String catalogName;  // TODO - Likely not needed. Can embed in the Request classes.
  private final FileIO fileIO;
  private TableMetadata currentMetadata;
  private String metadataFileLocation;

  protected RestTableOperations(
      HttpClient httpClient,
      Map<String, String> properties,
      FileIO fileIO,
      String catalogName,
      TableIdentifier tableIdentifier) {
    this.httpClient = httpClient;
    this.properties = properties;
    this.fileIO = fileIO;
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.fullTableName = String.format("%s.%s", catalogName, tableIdentifier);
  }

  // TODO - Probably remove this builder since `newTableOps` is used instead.
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public TableMetadata refresh() {
    throw new UnsupportedOperationException("Not implemented: refresh");
  }

  @Override
  protected void doRefresh() {
    throw new UnsupportedOperationException("Not implemented: doRefresh");
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  public void setCurrentMetadata(TableMetadata tableMetadata) {
    this.currentMetadata = tableMetadata;
  }

  public static class Builder {
    private TableIdentifier identifier;
    // TODO - Use the RestClient interface here instead.
    private HttpClient httpClient;
    private Map<String, String> properties;
    private String catalogName;  // TODO - Likely not needed. Can embed in the Request classes.
    private FileIO io;

    public Builder identifier(TableIdentifier tableIdentifier) {
      this.identifier = tableIdentifier;
      return this;
    }

    // TODO - Change to use interface
    public Builder httpClient(HttpClient client) {
      this.httpClient = client;
      return this;
    }

    public Builder properties(Map<String, String> props) {
      this.properties = props;
      return this;
    }

    public Builder catalogName(String name) {
      this.catalogName = name;
      return this;
    }

    public Builder fileIO(FileIO fileIO) {
      this.io = fileIO;
      return this;
    }

    public RestTableOperations build() {
      return new RestTableOperations(httpClient, properties, io, catalogName, identifier);
    }
  }
}
