/*
 *    Copyright 2021 Two Sigma Open Source, LLC
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.iceberg.jdbc.v2;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.v2.jooqgenerated.tables.records.NamespacesRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.convertCatalogDbExceptionToIcebergException;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.convertJSONStringToMap;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.convertMapToJSONString;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.generateShortUuid;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.getBucketNameFromUri;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.getLocationUri;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.namespaceToString;

public class JdbcCatalog extends BaseMetastoreCatalog implements AutoCloseable, SupportsNamespaces {

    public enum NamespaceCreationProperty {
        BACKEND("namespace_backend"),
        EXISTING_BUCKET("existing_bucket");

        private final String propName;

        NamespaceCreationProperty(String propName) {
            this.propName = propName;
        }

        public String getPropName() {
            return propName;
        }

        public String getPropValue(Map<String, String> props) {
            return props.get(propName);
        }
    }

    // Namespace properties with the following name were initialized at creation time and cannot be
    // changed/removed afterwards.
    private static final ImmutableSet<String> READ_ONLY_NAMESPACE_PROPS =
            ImmutableSet.copyOf(
                    Arrays.stream(NamespaceCreationProperty.values())
                            .map(p -> p.propName)
                            .collect(Collectors.toSet()));

    private static void validateNamespacePropertiesChange(Set<String> propsToChange) {
        for (String prop : propsToChange) {
            Preconditions.checkArgument(
                    !READ_ONLY_NAMESPACE_PROPS.contains(prop),
                    "Namespace property " + prop + " is read-only after namespace creation.");
        }
    }

    private static URI createBucket(
            String namespaceName, String projectId, S3BackendKind backendType) {
        String newBucket = namespaceName + "-" + generateShortUuid();
        AmazonS3 client = backendType.createS3Client(projectId);
        if (client.doesBucketExistV2(newBucket)) {
            throw new AlreadyExistsException(
                    String.format("Bucket %s exists on %s", namespaceName, backendType));
        }
        client.createBucket(newBucket);

        return getLocationUri(backendType.scheme, newBucket);
    }

    /**
     * Currently, this is not called when removing namespaces Rather, it is used when creating
     * namespace failed but bucket creation succeeded
     */
    private static void removeBucket(URI location, String projectId) {
        S3BackendKind backendType = S3BackendKind.fromScheme(location.getScheme());
        String bucketName = getBucketNameFromUri(location);
        AmazonS3 client = backendType.createS3Client(projectId);
        client.deleteBucket(bucketName);
    }

    // this IO can be used for more than one bucket
    private final FileIO io;
    private final CatalogDb db;
    // TODO this should be loaded from db or as part of bucket uri.
    @Nullable
    private final String projectId;

    public JdbcCatalog(CatalogDb db, FileIO fileIO, @Nullable String projectId) {
        this.db = db;
        this.io = fileIO;
        this.projectId = projectId;
    }

    @Override
    public void close() {
        this.db.close();
    }


    @Override
    public Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties) {
        // get the base location from the namespace location
        try {
            return this.buildTable(identifier, schema)
                    .withPartitionSpec(spec == null ? PartitionSpec.unpartitioned() : spec)
                    .withLocation(
                            location == null ? defaultWarehouseLocation(identifier) : location)
                    .withProperties(properties == null ? new HashMap<>() : properties)
                    .create();
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        return new JdbcTableOperations(io, tableIdentifier, db);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        String namespaceName = namespaceToString(tableIdentifier.namespace());
        try {
            return db.getNamespaceLocation(namespaceName) + "/" + tableIdentifier.name();
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    /**
     * Returns the latest snapshot ID for a given table.
     *
     * @param tableIdentifier Table to select the snapshot for.
     * @return Snapshot ID of latest snapshot.
     */
    public Optional<Long> getLatestSnapshot(TableIdentifier tableIdentifier) {
        TableOperations tableOperations = newTableOps(tableIdentifier);
        TableMetadata current = tableOperations.current();
        Snapshot snapshot = current != null ? current.currentSnapshot() : null;
        return snapshot != null ? Optional.of(snapshot.snapshotId()) : Optional.empty();
    }


    /**
     * Remove all table files with a certain prefix It will delete the objects one by one because
     * batch delete is not supported in GCS
     *
     * @param tableIdentifier the table to remove
     */
    private void purgeTableFiles(TableIdentifier tableIdentifier) {
        String namespaceName = namespaceToString(tableIdentifier.namespace());
        String tableName = tableIdentifier.name();

        URI namespaceLocation = URI.create(db.getNamespaceLocation(namespaceName));

        AmazonS3 client = S3BackendKind.fromScheme(namespaceLocation.getScheme()).createS3Client(projectId);
        String bucketName =
                getBucketNameFromUri(namespaceLocation);

        // remove based on a prefix
        List<S3ObjectSummary> summaries =
                S3BackendKind.globAllObjects(client, bucketName, tableName);
        List<String> allKeys =
                summaries.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
        S3BackendKind.deleteAllKeysDeleteObject(client, bucketName, allKeys);
    }

    /**
     * @param tableIdentifier Iceberg table identifier
     * @param purge whether to purge it, always except it to be false
     * @return boolean
     */
    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
        String namespaceName = namespaceToString(tableIdentifier.namespace());
        String tableName = tableIdentifier.name();

        /* TODO:
           1. Need to check the permission;
           2. put in the queue;
           3. all operation should be in one transaction
        */
        if (purge) {
            purgeTableFiles(tableIdentifier);
            // TODO: async purge not yet supported
        }
        try {
            db.dropTable(namespaceName, tableName);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
        return true;
    }

    /**
     * Change the name of the table in the catalog, note that the namespace has to stay the same.
     * The method will only change the name in the catalog, not the table name in S3.
     *
     * @param oldTableIdentifier the old identifier to change
     * @param newTableIdentifier the new table identifier
     */
    @Override
    public void renameTable(
            TableIdentifier oldTableIdentifier, TableIdentifier newTableIdentifier) {
        // TODO: Need to check permission on source and destination

        // get original table name
        String oldNamespaceStr = namespaceToString(oldTableIdentifier.namespace());
        String newNamespaceStr = namespaceToString(newTableIdentifier.namespace());

        String oldTableName = oldTableIdentifier.name();
        String newTableName = newTableIdentifier.name();

        try {
            db.renameTable(newNamespaceStr, oldTableName, newTableName);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    public boolean tableExists(TableIdentifier identifier) {
        try {
            return db.tableExists(namespaceToString(identifier.namespace()), identifier.name());
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    /**
     * TODO: Async Logic: 1. check privilege 2. find from the queue, dequeue the entry 3. check
     * whether purged 4. put the entry back to the database
     */
    public boolean undeleteTable() {
        // TODO: to implement
        throw new UnsupportedOperationException("Not supported yet");
    }

    /**
     * @param location full URI of the bucket, normally is the namespace name plus short UUID, but
     *     the user can also supply a specified bucket URI
     * @param namespaceProperties must include the backend type
     * @param removeBucketIfFail if create namespace failed with the database, remove the bucket
     */
    private void createNamespaceWithBucket(
            Namespace namespace,
            URI location,
            Map<String, String> namespaceProperties,
            boolean removeBucketIfFail) {
        String namespaceName = namespaceToString(namespace);
        // TODO: make sure this is atomic
        if (namespaceExists(namespace)) {
            throw new AlreadyExistsException("Namespace %s already exists", namespace);
        }

        NamespacesRecord namespacesRecord = new NamespacesRecord();

        namespacesRecord.setNamespaceName(namespaceName);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        namespacesRecord.setCreatedTimestamp(timestamp);
        namespacesRecord.setLastUpdatedTimestamp(timestamp);

        // TODO: set up permissions properly
        namespacesRecord.setOwnerGroup("test_group");
        namespacesRecord.setCreatedByUser("test_user");
        namespacesRecord.setLastUpdatedByUser("test_user");

        namespacesRecord.setLocation(location.toASCIIString());
        namespacesRecord.setProperties(convertMapToJSONString(namespaceProperties));

        try {
            db.insertNamespace(namespacesRecord);
        } catch (CatalogDbException e) {
            if (removeBucketIfFail) {
                removeBucket(location, projectId);
            }
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    /**
     * @param namespaceProperties must have backend type, when supplied with an existing bucket, the
     *     bucket of choice will be used, otherwise create a new bucket on the backend
     */
    @Override
    public void createNamespace(Namespace namespace, Map<String, String> namespaceProperties) {
        String namespaceName = namespaceToString(namespace);
        JdbcCatalogUtils.validateNamespaceName(namespaceName);

        // create bucket on backend
        S3BackendKind backend =
                S3BackendKind.fromName(
                        NamespaceCreationProperty.BACKEND.getPropValue(namespaceProperties));

        // TODO check pre-existing bucket exist and fail fast for user
        String preExistingBucket =
                NamespaceCreationProperty.EXISTING_BUCKET.getPropValue(namespaceProperties);
        boolean shouldCreateNewBucket = preExistingBucket == null;
        URI location =
                shouldCreateNewBucket
                        ? createBucket(namespaceName, projectId, backend)
                        : getLocationUri(backend.scheme, preExistingBucket);
        createNamespaceWithBucket(namespace, location, namespaceProperties, shouldCreateNewBucket);
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
        try {
            return db.namespaceExists(namespaceToString(namespace));
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        List<String> tableNames;
        try {
            tableNames = db.listTables(namespaceToString(namespace));
            return tableNames.stream()
                    .map(tableName -> TableIdentifier.of(namespace, tableName))
                    .collect(Collectors.toList());
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        } catch (RuntimeException e) {
            throw new RuntimeException("Table name to TableIdentifier conversion failed", e);
        }
    }

    /**
     * This one gets the subnamespaces, this is not what we want for list namespaces owned by a user
     * or all
     */
    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException(
                "List namespaces with prefix is not supported (yet or never?)");
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace)
            throws NoSuchNamespaceException {

        final String namespaceName = namespaceToString(namespace);

        final String propertiesString;
        try {
            propertiesString = db.getNamespacePropertiesString(namespaceName);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }

        return convertJSONStringToMap(propertiesString);
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> updateMap)
            throws NoSuchNamespaceException {
        validateNamespacePropertiesChange(updateMap.keySet());
        String namespaceName = namespaceToString(namespace);
        String oldPropertiesStr;
        try {
            oldPropertiesStr = db.getNamespacePropertiesString(namespaceName);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }

        Map<String, String> map = convertJSONStringToMap(oldPropertiesStr);
        map.putAll(updateMap); // merge the new hashmap to the old one
        String newPropertiesStr = convertMapToJSONString(map);

        try {
            db.setPropertiesStr(namespaceName, oldPropertiesStr, newPropertiesStr);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }

        return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> set)
            throws NoSuchNamespaceException {
        validateNamespacePropertiesChange(set);
        String namespaceName = namespaceToString(namespace);
        String oldPropertiesStr;
        try {
            oldPropertiesStr = db.getNamespacePropertiesString(namespaceName);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
        Map<String, String> map = convertJSONStringToMap(oldPropertiesStr);

        map.keySet().removeAll(set);
        String newPropertiesStr = convertMapToJSONString(map);

        try {
            db.setPropertiesStr(namespaceName, oldPropertiesStr, newPropertiesStr);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }

        return true;
    }


    /**
     * Admin operation
     *
     * @param namespace namespace to drop
     * @return if drop succeeded
     */
    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        // TODO: make sure user has the privilege to do so
        // FIXME: not transactional

        // TODO: do we want to purge the bucket? Current answer is NO
        if (!namespaceExists(namespace))
            throw new NoSuchNamespaceException("Namespace %s does not exist", namespace);

        try {
            if (!db.namespaceIsEmpty(namespaceToString(namespace)))
                throw new NamespaceNotEmptyException("Namespace %s not empty", namespace);
            db.dropNamespace(namespaceToString(namespace));
            return true;
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

}
