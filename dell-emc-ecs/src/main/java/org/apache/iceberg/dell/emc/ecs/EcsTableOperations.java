/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell.emc.ecs;

import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

/**
 * Table ops
 */
public class EcsTableOperations extends BaseMetastoreTableOperations {

    public static final String ICEBERG_METADATA_LOCATION = "icebergMetadataLocation";

    /**
     * table full name
     */
    private final String tableName;
    /**
     * table id
     */
    private final TableIdentifier id;
    /**
     * ecs client for table metadata
     */
    private final EcsClient ecs;
    /**
     * file io
     */
    private final FileIO io;
    private final PropertiesSerDes propertiesSerDes;

    /**
     * cached properties for CAS commit
     *
     * @see #doRefresh() when reset this field
     * @see #doCommit(TableMetadata, TableMetadata) when use this field
     */
    private Map<String, String> cachedProperties;

    public EcsTableOperations(String tableName, TableIdentifier id, EcsClient ecs, FileIO io, PropertiesSerDes propertiesSerDes) {
        this.tableName = tableName;
        this.id = id;
        this.ecs = ecs;
        this.io = io;
        this.propertiesSerDes = propertiesSerDes;
    }

    @Override
    protected String tableName() {
        return tableName;
    }

    @Override
    public FileIO io() {
        return io;
    }

    @Override
    protected void doRefresh() {
        String metadataLocation;
        ObjectKey metadataKey = ecs.getKeys().getMetadataKey(id);
        if (!ecs.head(metadataKey).isPresent()) {
            if (currentMetadataLocation() != null) {
                throw new NoSuchTableException("metadata is null");
            } else {
                metadataLocation = null;
            }
        } else {
            Map<String, String> metadata = queryProperties();
            this.cachedProperties = metadata;
            metadataLocation = metadata.get(ICEBERG_METADATA_LOCATION);
            if (metadataLocation == null) {
                throw new IllegalStateException("can't find location from table metadata");
            }
        }
        refreshFromMetadataLocation(metadataLocation);
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
        ObjectKey metadataKey = ecs.getKeys().getMetadataKey(id);
        int currentVersion = currentVersion();
        int nextVersion = currentVersion + 1;
        String newMetadataLocation = writeNewMetadata(metadata, nextVersion);
        if (base == null) {
            // create a new table, the metadataKey should be absent
            boolean r = ecs.writeIfAbsent(
                    metadataKey,
                    propertiesSerDes.toBytes(buildProperties(newMetadataLocation)));
            if (!r) {
                throw new CommitFailedException("table exist when commit %s(%s)", debug(metadata), newMetadataLocation);
            }
        } else {
            Map<String, String> properties = cachedProperties;
            if (properties == null) {
                throw new CommitFailedException("when commit, local metadata is null, %s(%s) -> %s(%s)",
                        debug(base), currentMetadataLocation(),
                        debug(metadata), newMetadataLocation);
            }
            // replace to a new version, the E-Tag should be present and matched
            boolean r = ecs.replace(
                    metadataKey,
                    properties.get(PropertiesSerDes.ECS_OBJECT_E_TAG),
                    propertiesSerDes.toBytes(buildProperties(newMetadataLocation))
            );
            if (!r) {
                throw new CommitFailedException("replace failed, properties %s, %s(%s) -> %s(%s)", properties,
                        debug(base), currentMetadataLocation(),
                        debug(metadata), newMetadataLocation);
            }
        }
    }

    private Map<String, String> queryProperties() {
        EcsClient.ContentAndETag contentAndETag = ecs.readAll(ecs.getKeys().getMetadataKey(id));
        return propertiesSerDes.readProperties(contentAndETag);
    }

    /**
     * debug string for exception
     *
     * @param metadata is table metadata
     * @return debug string of metadata
     */
    private String debug(TableMetadata metadata) {
        return "Table(currentSnapshotId = " + metadata.currentSnapshot().snapshotId() + ")";
    }

    /**
     * build a new properties for table
     *
     * @param metadataLocation is metadata json file location
     * @return properties
     */
    private Map<String, String> buildProperties(String metadataLocation) {
        return Collections.singletonMap(ICEBERG_METADATA_LOCATION, metadataLocation);
    }
}