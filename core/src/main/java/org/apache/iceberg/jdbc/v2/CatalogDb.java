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

package org.apache.iceberg.jdbc.v2;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.jdbc.v2.jooqgenerated.tables.records.NamespacesRecord;
import org.apache.iceberg.jdbc.v2.jooqgenerated.tables.records.TablesRecord;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;

import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.isIntegrityException;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.validateNamespaceName;
import static org.apache.iceberg.jdbc.v2.JdbcCatalogUtils.validateTableName;
import static org.apache.iceberg.jdbc.v2.jooqgenerated.Tables.NAMESPACES;
import static org.apache.iceberg.jdbc.v2.jooqgenerated.Tables.TABLES;

/** Encapsulate all the catalog database operations */
public class CatalogDb implements AutoCloseable {
    private final DataSource dataSource;
    private final SQLDialect dialect;

    @Override
    public void close() {
        dataSource.close();
    }

    public enum DbType {
        postgres("org.postgresql.Driver", "jdbc:postgresql://%s:%d/%s", SQLDialect.POSTGRES_10) {
            @Override
            public String getUrl(String host, String port, String dbName) {
                Preconditions.checkArgument(host != null);
                Preconditions.checkArgument(port != null);
                Preconditions.checkArgument(dbName != null);

                return String.format(this.formatter, host, Integer.parseInt(port), dbName);
            }
        },
        h2(
                "org.h2.Driver",
                "jdbc:h2:mem:%s;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE",
                SQLDialect.POSTGRES_10) {
            @Override
            public String getUrl(String host, String port, String dbName) {
                Preconditions.checkArgument(dbName != null);

                return String.format(this.formatter, dbName);
            }
        };

        final String driver;
        final String formatter;
        final SQLDialect dialect;

        DbType(String driver, String formatter, SQLDialect dialect) {
            this.driver = driver;
            this.formatter = formatter;
            this.dialect = dialect;
        }

        public abstract String getUrl(String host, String port, String dbName);
    }

    private CatalogDb(DataSource dataSource, SQLDialect dialect) {
        this.dataSource = dataSource;
        this.dialect = dialect;
    }

    public static CatalogDb createCatalogDb(DbType dbType, PoolProperties p) {
        final DataSource d = new DataSource();
        d.setPoolProperties(p);
        return new CatalogDb(d, dbType.dialect);
    }

    public static CatalogDb createCatalogDb(
            DbType dbType, String host, String port, String dbName) {

        // construct  PoolProperties
        final PoolProperties p = new PoolProperties();

        p.setUrl(dbType.getUrl(host, port, dbName));
        p.setDriverClassName(dbType.driver);

        // set default values
        p.setDefaultAutoCommit(true);
        p.setTestOnBorrow(true);
        p.setTestWhileIdle(true);
        p.setMaxIdle(20);
        p.setMinIdle(5);
        p.setMaxAge(TimeUnit.HOURS.toMillis(1));

        return createCatalogDb(dbType, p);
    }

    @VisibleForTesting
    DSLContext getContext() {
        return DSL.using(new DataSourceConnectionProvider(this.dataSource), this.dialect);
    }

    void renameTable(String namespaceName, String oldTableName, String newTableName) {
        validateTableName(newTableName);

        try {
            final int updated =
                    getContext()
                            .update(TABLES)
                            .set(TABLES.TABLE_NAME, newTableName)
                            .where(
                                    TABLES.TABLE_NAME
                                            .eq(oldTableName)
                                            .and(TABLES.NAMESPACE_NAME.eq(namespaceName)))
                            .execute();
            if (updated == 0) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.TABLE_NOT_FOUND,
                        namespaceName,
                        oldTableName,
                        null);
            }
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    void insertTable(TablesRecord table) {
        final String tableName = table.getTableName();
        final String namespaceName = table.getNamespaceName();
        validateTableName(tableName);

        // FIXME: make transactional
        if (!namespaceExists(namespaceName)) {
            throw new CatalogDbException(
                    CatalogDbException.ErrorCode.NAMESPACE_NOT_FOUND, namespaceName, null);
        }

        try {
            final int count = getContext().executeInsert(table);
            if (count != 1) {
                throw new CatalogDbException(
                        String.format(
                                "Table %s in namespace %s insertion failed.",
                                tableName, namespaceName),
                        null);
            }
        } catch (DataAccessException e) {
            if (isIntegrityException(e)) {
                // table primary key (NAMESPACE, TABLE_NAME) exists
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.TABLE_EXISTS, namespaceName, tableName, e);
            } else {
                // other exceptions are not expected
                throw new CatalogDbException(e);
            }
        }
    }

    /** Return metadata of a table */
    TablesRecord getTable(String namespaceName, String tableName) {
        // get table metadata (including table location)
        try {
            final TablesRecord r =
                    getContext()
                            .select()
                            .from(TABLES)
                            .where(
                                    TABLES.NAMESPACE_NAME
                                            .eq(namespaceName)
                                            .and(TABLES.TABLE_NAME.eq(tableName)))
                            .fetchOneInto(TablesRecord.class);
            if (r == null) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.TABLE_NOT_FOUND,
                        namespaceName,
                        tableName,
                        null);
            }
            return r;
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    String getTablePointer(String namespaceName, String tableName) {
        final DSLContext c = getContext();
        try {
            final String pointer =
                    c.select(TABLES.TABLE_POINTER)
                            .from(TABLES)
                            .where(
                                    TABLES.NAMESPACE_NAME
                                            .eq(namespaceName)
                                            .and(TABLES.TABLE_NAME.eq(tableName)))
                            .fetchOne(TABLES.TABLE_POINTER);
            if (pointer == null) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.TABLE_NOT_FOUND,
                        namespaceName,
                        tableName,
                        null);
            }
            return pointer;
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    void updateTable(
            String namespaceName,
            String tableName,
            String oldPointer,
            String newPointer,
            String lastUpdatedByUser) {
        final Timestamp lastUpdatedTimestamp = new Timestamp(System.currentTimeMillis());
        final DSLContext c = getContext();
        // update where(OLD PROPERTIES) to make sure atomic rather than using transaction
        try {
            int updated =
                    c.update(TABLES)
                            .set(TABLES.TABLE_POINTER, newPointer)
                            .set(TABLES.LAST_UPDATED_TIMESTAMP, lastUpdatedTimestamp)
                            .set(TABLES.LAST_UPDATED_BY_USER, lastUpdatedByUser)
                            .where(
                                    TABLES.NAMESPACE_NAME
                                            .eq(namespaceName)
                                            .and(TABLES.TABLE_NAME.eq(tableName))
                                            .and(TABLES.TABLE_POINTER.eq(oldPointer)))
                            .execute();
            if (updated == 0) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.TABLE_NOT_FOUND_OR_STALE,
                        namespaceName,
                        tableName,
                        null);
            }
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    boolean tableExists(String namespaceName, String tableName) {
        final DSLContext c = getContext();
        try {
            return c.fetchExists(
                    c.selectOne()
                            .from(TABLES)
                            .where(
                                    TABLES.NAMESPACE_NAME
                                            .eq(namespaceName)
                                            .and(TABLES.TABLE_NAME.eq(tableName))));
        } catch (DataAccessException e) {
            // unexpected error
            throw new CatalogDbException(e);
        }
    }

    void dropTable(String namespaceName, String tableName) {
        try {
            final int deleted =
                    getContext()
                            .delete(TABLES)
                            .where(
                                    TABLES.NAMESPACE_NAME
                                            .eq(namespaceName)
                                            .and(TABLES.TABLE_NAME.eq(tableName)))
                            .execute();
            if (deleted == 0) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.TABLE_NOT_FOUND,
                        namespaceName,
                        tableName,
                        null);
            }
        } catch (DataAccessException e) {
            // unexpected error
            throw new CatalogDbException(e);
        }
    }

    /**
     * @param namespaceName namespace name
     * @return can return an empty list if no table in a namespace
     */
    List<String> listTables(String namespaceName) {
        final DSLContext c = getContext();
        if (!namespaceExists(namespaceName)) {
            throw new CatalogDbException(
                    CatalogDbException.ErrorCode.NAMESPACE_NOT_FOUND, namespaceName, null);
        }
        try {
            final Result<Record> results =
                    c.select().from(TABLES).where(TABLES.NAMESPACE_NAME.eq(namespaceName)).fetch();

            return results.getValues(TABLES.TABLE_NAME);
        } catch (DataAccessException e) {
            // unexpected error
            throw new CatalogDbException(e);
        }
    }

    void insertNamespace(NamespacesRecord namespacesRecord) {
        validateNamespaceName(namespacesRecord.getNamespaceName());
        try {
            final int count = getContext().executeInsert(namespacesRecord);
            if (count != 1) {
                throw new CatalogDbException(
                        String.format(
                                "Namespace %s insertion failed.",
                                namespacesRecord.getNamespaceName()),
                        null);
            }
        } catch (DataAccessException e) {
            if (isIntegrityException(e)) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.NAMESPACE_EXISTS,
                        namespacesRecord.getNamespaceName(),
                        e);
            }
            throw new CatalogDbException(e);
        }
    }

    boolean namespaceExists(String namespaceName) {
        final DSLContext c = getContext();
        try {
            return c.fetchExists(
                    c.selectOne()
                            .from(NAMESPACES)
                            .where(NAMESPACES.NAMESPACE_NAME.eq(namespaceName)));
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    /**
     * Check whether a namespace is empty
     *
     * @param namespaceName the namespace to check
     * @return true for empty
     */
    boolean namespaceIsEmpty(String namespaceName) {
        final DSLContext c = getContext();
        try {
            return !c.fetchExists(
                    c.selectOne().from(TABLES).where(TABLES.NAMESPACE_NAME.eq(namespaceName)));
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    String getNamespaceLocation(String namespaceName) {
        final DSLContext c = getContext();
        try {
            String location =
                    c.select(NAMESPACES.LOCATION)
                            .from(NAMESPACES)
                            .where(NAMESPACES.NAMESPACE_NAME.eq(namespaceName))
                            .fetchOne(NAMESPACES.LOCATION);
            if (location == null) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.NAMESPACE_NOT_FOUND, namespaceName, null);
            }
            return location;
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    /**
     * Admin operation
     *
     * @return All the namespaces in a catalog
     */
    List<String> listAllNamespaces() {
        final DSLContext c = getContext();
        try {
            final Result<Record> results = c.select().from(NAMESPACES).fetch();
            return results.getValues(NAMESPACES.NAMESPACE_NAME);
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    /**
     * Admin operation
     *
     * @param namespaceName namespace name to drop
     */
    void dropNamespace(String namespaceName) {
        final DSLContext c = getContext();
        try {
            c.delete(NAMESPACES).where(NAMESPACES.NAMESPACE_NAME.eq(namespaceName)).execute();
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    // get the original properties JSON string from namespace entry
    String getNamespacePropertiesString(String namespaceName) {
        final DSLContext c = getContext();
        try {
            String properties =
                    c.select(NAMESPACES.PROPERTIES)
                            .from(NAMESPACES)
                            .where(NAMESPACES.NAMESPACE_NAME.eq(namespaceName))
                            .fetchOne(NAMESPACES.PROPERTIES);
            if (properties == null) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.NAMESPACE_NOT_FOUND, namespaceName, null);
            }

            return properties;
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    void setPropertiesStr(String namespaceName, String oldPropertiesStr, String newPropertiesStr) {
        final DSLContext c = getContext();
        // update where(OLD PROPERTIES) to make sure atomic rather than using transaction
        try {
            int updated =
                    c.update(NAMESPACES)
                            .set(NAMESPACES.PROPERTIES, newPropertiesStr)
                            .where(
                                    NAMESPACES
                                            .NAMESPACE_NAME
                                            .eq(namespaceName)
                                            .and(NAMESPACES.PROPERTIES.eq(oldPropertiesStr)))
                            .execute();
            // either namespace constraint failed, or the property constraint failed
            if (updated == 0) {
                throw new CatalogDbException(
                        CatalogDbException.ErrorCode.NAMESPACE_NOT_FOUND_OR_STALE,
                        namespaceName,
                        null);
            }
        } catch (DataAccessException e) {
            throw new CatalogDbException(e);
        }
    }

    /** FIXME: Only for testing; add another interface for testing and admin operations */
    void dropAll() {
        getContext().deleteFrom(TABLES).execute();
        getContext().deleteFrom(NAMESPACES).execute();
    }
}
