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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.http.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO - Extract out to an interface - Implement with HTTP version.
// TODO - Provide Builder interface - Implement with HTTP version.
// TODO - Should we implement Configurable here? Since this will be an interface, I think not in the interface.
// TODO - As this will be more of an interface, possibly extend TableOperations directly (like HadoopTableOperations)
class RestTableOperations extends BaseMetastoreTableOperations implements Closeable, SupportsNamespaces, Configurable {

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
  private Configuration hadoopConf;

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

  public static Builder builder() {
    return new Builder();
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

  @Override
  public TableMetadata refresh() {
    throw new UnsupportedOperationException("Not implemented: refresh");
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf;
  }

  public void setCurrentMetadata(TableMetadata tableMetadata) {
    this.currentMetadata = tableMetadata;
  }

  @Override
  public void createNamespace(Namespace namespace) {
    throw new UnsupportedOperationException("Not implemented: createNamespace");
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    throw new UnsupportedOperationException("Not implemented: createNamespace");
  }

  @Override
  public List<Namespace> listNamespaces() {
    throw new UnsupportedOperationException("Not implemented: listNamespaces");
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: listNamespaces");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: loadNamespaceMetadata");
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    throw new UnsupportedOperationException("Not implemented: dropNamespace");
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> props) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: setProperties");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> props) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: removeProperties");
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    throw new UnsupportedOperationException("Not implemented: namespaceExists");
  }
}
