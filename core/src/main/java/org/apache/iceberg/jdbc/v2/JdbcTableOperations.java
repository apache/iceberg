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

import java.sql.Timestamp;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.v2.jooqgenerated.tables.records.TablesRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.convertCatalogDbExceptionToIcebergException;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.namespaceToString;

public class JdbcTableOperations extends BaseMetastoreTableOperations {
    private static final String SCHEME = "iceberg://";

    public static class CatalogTableEntry {
        public final String tablePointer;
        public final Timestamp createdTimestamp;
        public final String createdByUser;
        public final Timestamp lastUpdatedTimestamp;
        public final String lastUpdatedByUser;

        public CatalogTableEntry(
                String tablePointer,
                Timestamp createdTimestamp,
                String createdByUser,
                Timestamp lastUpdatedTimestamp,
                String lastUpdatedByUser) {
            this.tablePointer = tablePointer;
            this.createdTimestamp = createdTimestamp;
            this.createdByUser = createdByUser;
            this.lastUpdatedTimestamp = lastUpdatedTimestamp;
            this.lastUpdatedByUser = lastUpdatedByUser;
        }
    }

    private final FileIO io;
    private final TableIdentifier tableIdentifier;
    private final CatalogDb db;

    protected JdbcTableOperations(FileIO io, TableIdentifier tableIdentifier, CatalogDb db) {
        this.io = io;
        this.tableIdentifier = tableIdentifier;
        this.db = db;
    }

    /**
     * This is called internally by Iceberg when committing a table or creating a new table The
     * table metadata file is created optimistically, then the record is put in the database. If the
     * database update failed (mainly because other writers commit first), this will not retry. To
     * add retry logic, we need to refresh (to update current version) and start over from writing
     * new metadata file again.
     */
    @Override
    protected void doCommit(TableMetadata base, TableMetadata metadata) {
        Preconditions.checkArgument(io() != null, "io is empty");

        final String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

        // create a new table in the catalog
        if (base == null) {
            createTableEntry(newMetadataLocation);
        } else {
            // update the table pointer of an existing catalog
            String oldMetadataLocation = base.metadataFileLocation();
            updateTable(oldMetadataLocation, newMetadataLocation);
        }
    }

    /**
     * This method is not used anywhere
     *
     * @return table name as a URI with certain scheme.
     */
    @Override
    protected String tableName() {
        return SCHEME
                + namespaceToString(tableIdentifier.namespace())
                + "/"
                + tableIdentifier.name();
    }

    /**
     * Get the location from the database and refresh. Iceberg might retry several times if this
     * fails
     */
    @Override
    protected void doRefresh() {
        String tablePointer;
        try {
            tablePointer =
                    db.getTablePointer(
                            namespaceToString(tableIdentifier.namespace()), tableIdentifier.name());
        } catch (CatalogDbException e) {
            // if table not found, we can set it to null allow the create table method to work
            if (e.getErrorCode() == CatalogDbException.ErrorCode.TABLE_NOT_FOUND) {
                tablePointer = null;
            } else {
                throw e;
            }
        }
        refreshFromMetadataLocation(tablePointer);
    }

    @Override
    public FileIO io() {
        return io;
    }

    /**
     * Create a table entry in the catalog database
     *
     * @param tablePointer the table pointer of the just created table
     */
    protected void createTableEntry(String tablePointer) {
        final TablesRecord table = new TablesRecord();

        final String namespaceStr = namespaceToString(tableIdentifier.namespace());
        final String tableName = tableIdentifier.name();
        table.setTableName(tableName);
        table.setNamespaceName(namespaceStr);

        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        table.setCreatedTimestamp(timestamp);
        table.setLastUpdatedTimestamp(timestamp);

        // TODO: get this from authentication module
        table.setCreatedByUser("test_user");
        table.setLastUpdatedByUser("test_user");

        table.setTablePointer(tablePointer);
        try {
            db.insertTable(table);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    protected void updateTable(String oldPointer, String newPointer) {
        // TODO: get current user in a reliable way
        final String user = "test_user";
        try {
            db.updateTable(
                    namespaceToString(tableIdentifier.namespace()),
                    tableIdentifier.name(),
                    oldPointer,
                    newPointer,
                    user);
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }

    /**
     * This can be modified to serve future need for the table metadata
     *
     * @return A metadata struct (CatalogTableEntry) for the table
     */
    CatalogTableEntry getTable() {
        try {
            final TablesRecord record =
                    db.getTable(
                            namespaceToString(tableIdentifier.namespace()), tableIdentifier.name());
            return new CatalogTableEntry(
                    record.getTablePointer(),
                    record.getCreatedTimestamp(),
                    record.getCreatedByUser(),
                    record.getLastUpdatedTimestamp(),
                    record.getLastUpdatedByUser());
        } catch (CatalogDbException e) {
            throw convertCatalogDbExceptionToIcebergException(e);
        }
    }
}
