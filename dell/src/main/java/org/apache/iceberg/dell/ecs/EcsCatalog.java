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

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.DellClientFactories;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EcsCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable<Object> {

  /** Suffix of table metadata object */
  private static final String TABLE_OBJECT_SUFFIX = ".table";

  /** Suffix of namespace metadata object */
  private static final String NAMESPACE_OBJECT_SUFFIX = ".namespace";

  /** Key of properties version in ECS object user metadata. */
  private static final String PROPERTIES_VERSION_USER_METADATA_KEY = "iceberg_properties_version";

  private static final Logger LOG = LoggerFactory.getLogger(EcsCatalog.class);

  private S3Client client;
  private Object hadoopConf;
  private String catalogName;

  /** Warehouse is unified with other catalog that without delimiter. */
  private EcsURI warehouseLocation;

  private FileIO fileIO;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;

  /**
   * No-arg constructor to load the catalog dynamically.
   *
   * <p>All fields are initialized by calling {@link EcsCatalog#initialize(String, Map)} later.
   */
  public EcsCatalog() {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogProperties = ImmutableMap.copyOf(properties);
    String inputWarehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(inputWarehouseLocation),
        "Cannot initialize EcsCatalog because warehousePath must not be null or empty");

    this.catalogName = name;
    this.warehouseLocation = new EcsURI(LocationUtil.stripTrailingSlash(inputWarehouseLocation));
    this.client = DellClientFactories.from(properties).ecsS3();
    this.fileIO = initializeFileIO(properties);

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(client::destroy);
    closeableGroup.addCloseable(fileIO);
    closeableGroup.setSuppressCloseFailure(true);
  }

  private FileIO initializeFileIO(Map<String, String> properties) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      FileIO io = new EcsFileIO();
      io.initialize(properties);
      return io;
    } else {
      return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new EcsTableOperations(
        String.format("%s.%s", catalogName, tableIdentifier),
        tableURI(tableIdentifier),
        fileIO,
        this);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return String.format(
        "%s/%s", namespacePrefix(tableIdentifier.namespace()), tableIdentifier.name());
  }

  /** Iterate all table objects with the namespace prefix. */
  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespace.isEmpty() && !namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace %s does not exist", namespace);
    }

    String marker = null;
    List<TableIdentifier> results = Lists.newArrayList();
    // Add the end slash when delimiter listing
    EcsURI prefix = new EcsURI(String.format("%s/", namespacePrefix(namespace)));
    do {
      ListObjectsResult listObjectsResult =
          client.listObjects(
              new ListObjectsRequest(prefix.bucket())
                  .withDelimiter("/")
                  .withPrefix(prefix.name())
                  .withMarker(marker));
      marker = listObjectsResult.getNextMarker();
      results.addAll(
          listObjectsResult.getObjects().stream()
              .filter(s3Object -> s3Object.getKey().endsWith(TABLE_OBJECT_SUFFIX))
              .map(object -> parseTableId(namespace, prefix, object))
              .collect(Collectors.toList()));
    } while (marker != null);

    LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, results);
    return results;
  }

  /** Get object prefix of namespace without the end slash. */
  private String namespacePrefix(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation.location();
    } else {
      // If the warehouseLocation.name is empty, the leading slash will be ignored
      return String.format(
          "%s/%s", warehouseLocation.location(), String.join("/", namespace.levels()));
    }
  }

  private TableIdentifier parseTableId(Namespace namespace, EcsURI prefix, S3Object s3Object) {
    String key = s3Object.getKey();
    Preconditions.checkArgument(
        key.startsWith(prefix.name()), "List result should have same prefix", key, prefix);

    String tableName =
        key.substring(prefix.name().length(), key.length() - TABLE_OBJECT_SUFFIX.length());
    return TableIdentifier.of(namespace, tableName);
  }

  /** Remove table object. If the purge flag is set, remove all data objects. */
  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!tableExists(identifier)) {
      return false;
    }

    EcsURI tableObjectURI = tableURI(identifier);
    if (purge) {
      // if re-use the same instance, current() will throw exception.
      TableOperations ops = newTableOps(identifier);
      TableMetadata current = ops.current();
      if (current == null) {
        return false;
      }

      CatalogUtil.dropTableData(ops.io(), current);
    }

    client.deleteObject(tableObjectURI.bucket(), tableObjectURI.name());
    return true;
  }

  private EcsURI tableURI(TableIdentifier id) {
    return new EcsURI(
        String.format("%s/%s%s", namespacePrefix(id.namespace()), id.name(), TABLE_OBJECT_SUFFIX));
  }

  /**
   * Table rename will only move table object, the data objects will still be in-place.
   *
   * @param from identifier of the table to rename
   * @param to new table name
   */
  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!namespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException(
          "Cannot rename %s to %s because namespace %s does not exist", from, to, to.namespace());
    }

    if (tableExists(to)) {
      throw new AlreadyExistsException(
          "Cannot rename %s because destination table %s exists", from, to);
    }

    EcsURI fromURI = tableURI(from);
    if (!objectMetadata(fromURI).isPresent()) {
      throw new NoSuchTableException("Cannot rename table because table %s does not exist", from);
    }

    Properties properties = loadProperties(fromURI);
    EcsURI toURI = tableURI(to);

    if (!putNewProperties(toURI, properties.content())) {
      throw new AlreadyExistsException(
          "Cannot rename %s because destination table %s exists", from, to);
    }

    client.deleteObject(fromURI.bucket(), fromURI.name());
    LOG.info("Rename table {} to {}", from, to);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> properties) {
    EcsURI namespaceObject = namespaceURI(namespace);
    if (!putNewProperties(namespaceObject, properties)) {
      throw new AlreadyExistsException(
          "namespace %s(%s) has already existed", namespace, namespaceObject);
    }
  }

  private EcsURI namespaceURI(Namespace namespace) {
    return new EcsURI(String.format("%s%s", namespacePrefix(namespace), NAMESPACE_OBJECT_SUFFIX));
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespace.isEmpty() && !namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace %s does not exist", namespace);
    }

    String marker = null;
    List<Namespace> results = Lists.newArrayList();
    // Add the end slash when delimiter listing
    EcsURI prefix = new EcsURI(String.format("%s/", namespacePrefix(namespace)));
    do {
      ListObjectsResult listObjectsResult =
          client.listObjects(
              new ListObjectsRequest(prefix.bucket())
                  .withDelimiter("/")
                  .withPrefix(prefix.name())
                  .withMarker(marker));
      marker = listObjectsResult.getNextMarker();
      results.addAll(
          listObjectsResult.getObjects().stream()
              .filter(s3Object -> s3Object.getKey().endsWith(NAMESPACE_OBJECT_SUFFIX))
              .map(object -> parseNamespace(namespace, prefix, object))
              .collect(Collectors.toList()));
    } while (marker != null);

    LOG.debug("Listing namespace {} returned namespaces: {}", namespace, results);
    return results;
  }

  private Namespace parseNamespace(Namespace parent, EcsURI prefix, S3Object s3Object) {
    String key = s3Object.getKey();
    Preconditions.checkArgument(
        key.startsWith(prefix.name()), "List result should have same prefix", key, prefix);

    String namespaceName =
        key.substring(prefix.name().length(), key.length() - NAMESPACE_OBJECT_SUFFIX.length());
    String[] namespace = Arrays.copyOf(parent.levels(), parent.levels().length + 1);
    namespace[namespace.length - 1] = namespaceName;
    return Namespace.of(namespace);
  }

  /** Load namespace properties. */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    EcsURI namespaceObject = namespaceURI(namespace);
    if (!objectMetadata(namespaceObject).isPresent()) {
      throw new NoSuchNamespaceException(
          "Namespace %s(%s) properties object is absent", namespace, namespaceObject);
    }

    Map<String, String> result = loadProperties(namespaceObject).content();

    LOG.debug("Loaded metadata for namespace {} found {}", namespace, result);
    return result;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!namespace.isEmpty() && !namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace %s does not exist", namespace);
    }

    if (!listNamespaces(namespace).isEmpty() || !listTables(namespace).isEmpty()) {
      throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);
    }

    EcsURI namespaceObject = namespaceURI(namespace);
    client.deleteObject(namespaceObject.bucket(), namespaceObject.name());
    LOG.info("Dropped namespace: {}", namespace);
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    return updateProperties(namespace, r -> r.putAll(properties));
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    return updateProperties(namespace, r -> r.keySet().removeAll(properties));
  }

  public boolean updateProperties(Namespace namespace, Consumer<Map<String, String>> propertiesFn)
      throws NoSuchNamespaceException {

    // Load old properties
    Properties oldProperties = loadProperties(namespaceURI(namespace));

    // Put new properties
    Map<String, String> newProperties = new LinkedHashMap<>(oldProperties.content());
    propertiesFn.accept(newProperties);
    LOG.debug("Successfully set properties {} for {}", newProperties.keySet(), namespace);
    return updatePropertiesObject(namespaceURI(namespace), oldProperties.eTag(), newProperties);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return objectMetadata(namespaceURI(namespace)).isPresent();
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return objectMetadata(tableURI(identifier)).isPresent();
  }

  private void checkURI(EcsURI uri) {
    Preconditions.checkArgument(
        uri.bucket().equals(warehouseLocation.bucket()),
        "Properties object %s should be in same bucket %s",
        uri.location(),
        warehouseLocation.bucket());
    Preconditions.checkArgument(
        uri.name().startsWith(warehouseLocation.name()),
        "Properties object %s should have the expected prefix %s",
        uri.location(),
        warehouseLocation.name());
  }

  /** Get S3 object metadata which include E-Tag, user metadata and so on. */
  public Optional<S3ObjectMetadata> objectMetadata(EcsURI uri) {
    checkURI(uri);
    try {
      return Optional.of(client.getObjectMetadata(uri.bucket(), uri.name()));
    } catch (S3Exception e) {
      if (e.getHttpCode() == 404) {
        return Optional.empty();
      }

      throw e;
    }
  }

  /** Record class of properties content and E-Tag */
  static class Properties {
    private final String eTag;
    private final Map<String, String> content;

    Properties(String eTag, Map<String, String> content) {
      this.eTag = eTag;
      this.content = content;
    }

    public String eTag() {
      return eTag;
    }

    public Map<String, String> content() {
      return content;
    }
  }

  /** Parse object content and metadata as properties. */
  Properties loadProperties(EcsURI uri) {
    checkURI(uri);
    GetObjectResult<InputStream> result = client.getObject(uri.bucket(), uri.name());
    S3ObjectMetadata objectMetadata = result.getObjectMetadata();
    String version = objectMetadata.getUserMetadata(PROPERTIES_VERSION_USER_METADATA_KEY);
    Map<String, String> content;
    try (InputStream input = result.getObject()) {
      content = PropertiesSerDesUtil.read(ByteStreams.toByteArray(input), version);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return new Properties(objectMetadata.getETag(), content);
  }

  /** Create a new object to store properties. */
  boolean putNewProperties(EcsURI uri, Map<String, String> properties) {
    checkURI(uri);
    PutObjectRequest request =
        new PutObjectRequest(uri.bucket(), uri.name(), PropertiesSerDesUtil.toBytes(properties));
    request.setObjectMetadata(
        new S3ObjectMetadata()
            .addUserMetadata(
                PROPERTIES_VERSION_USER_METADATA_KEY, PropertiesSerDesUtil.currentVersion()));
    request.setIfNoneMatch("*");
    try {
      client.putObject(request);
      return true;
    } catch (S3Exception e) {
      if ("PreconditionFailed".equals(e.getErrorCode())) {
        return false;
      }

      throw e;
    }
  }

  /** Update a exist object to store properties. */
  boolean updatePropertiesObject(EcsURI uri, String eTag, Map<String, String> properties) {
    checkURI(uri);
    // Exclude some keys
    Map<String, String> newProperties = new LinkedHashMap<>(properties);

    // Replace properties object
    PutObjectRequest request =
        new PutObjectRequest(uri.bucket(), uri.name(), PropertiesSerDesUtil.toBytes(newProperties));
    request.setObjectMetadata(
        new S3ObjectMetadata()
            .addUserMetadata(
                PROPERTIES_VERSION_USER_METADATA_KEY, PropertiesSerDesUtil.currentVersion()));
    request.setIfMatch(eTag);
    try {
      client.putObject(request);
      return true;
    } catch (S3Exception e) {
      if ("PreconditionFailed".equals(e.getErrorCode())) {
        return false;
      }

      throw e;
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closeableGroup.close();
  }

  @Override
  public void setConf(Object conf) {
    this.hadoopConf = conf;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }
}
