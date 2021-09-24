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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.SQLStateClass;
import org.jooq.impl.DSL;

import static org.apache.iceberg.jdbc.v2.jooqgenerated.Tables.NAMESPACES;
import static org.apache.iceberg.jdbc.v2.jooqgenerated.Tables.TABLES;
import static org.jooq.impl.DSL.primaryKey;

final class JdbcCatalogUtils {

    private static final Joiner DOT = Joiner.on('.');
    static final int UUID_LENGTH = 8;

    static String namespaceToString(Namespace namespace) {
        return DOT.join(namespace.levels());
    }

    static boolean isIntegrityException(DataAccessException e) {
        return e.sqlStateClass().equals(SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION);
    }

    static RuntimeException convertCatalogDbExceptionToIcebergException(CatalogDbException e) {
        switch (e.getErrorCode()) {
            case NAMESPACE_NOT_FOUND:
                return new NoSuchNamespaceException(e, e.getMessage());
            case TABLE_NOT_FOUND:
                return new NoSuchTableException(e, e.getMessage());
            case TABLE_EXISTS:
                return new AlreadyExistsException(e, e.getMessage());
            case TABLE_NOT_FOUND_OR_STALE:
                return new CommitFailedException(e, e.getMessage());
            case NAMESPACE_EXISTS:
                // no corresponding iceberg exception
            case NAMESPACE_NOT_FOUND_OR_STALE:
                // no corresponding iceberg exception
            case UNKNOWN_ERR:
                // no corresponding iceberg exception
            default:
                return e;
        }
    }

    private static final RandomStringGenerator randomStringGenerator =
            new RandomStringGenerator.Builder()
                    .withinRange('0', 'z')
                    .filteredBy(
                            CharacterPredicates.ASCII_LOWERCASE_LETTERS, CharacterPredicates.DIGITS)
                    .build();

    static String generateShortUuid() {
        return randomStringGenerator.generate(UUID_LENGTH);
    }

    static URI getLocationUri(String scheme, String bucket) {
        try {
            return new URI(scheme, bucket, null, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                    String.format("URI syntax exception, scheme=%s, bucket=%s", scheme, bucket), e);
        }
    }

    static String getBucketNameFromUri(URI location) {
        return location.getAuthority();
    }

    /** By default, create a {@link HadoopFileIO} if no impl is specified in properties. */
    static FileIO createFileIO(Configuration conf, Map<String, String> properties) {
        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        return fileIOImpl == null
                ? new HadoopFileIO(conf)
                : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
    }

    static void validateTableName(String tableName) {
        // TODO: add more constraints of the table name
        if (tableName == null || tableName.isBlank()) {
            throw new CatalogDbException(
                    String.format("Table name \"%s\" is not proper", tableName), null);
        }
    }

    /**
     * Validate the namespace according to S3 and GCS rules.
     *
     * <p>From S3:
     *
     * <p>Bucket names must be between 3 and 63 characters long.
     *
     * <p>Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
     *
     * <p>Bucket names must begin and end with a letter or number.
     *
     * <p>Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
     *
     * <p>From GCS:
     *
     * <p>Bucket names cannot begin with the "goog" prefix.
     *
     * <p>Bucket names cannot contain "google" or close misspellings, such as "g00gle".
     *
     * <p>https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
     * https://cloud.google.com/storage/docs/naming-buckets#requirements
     */
    static void validateNamespaceName(String namespace) {
        Pattern allowedCharactersPattern = Pattern.compile("^[a-z0-9][a-z0-9.-]*$");
        Matcher matcher = allowedCharactersPattern.matcher(namespace);
        if (namespace.isBlank()
                || !matcher.matches()
                || namespace.length() > 63 - UUID_LENGTH - 1
                || InetAddresses.isInetAddress(namespace)
                || namespace.contains("..")
                || namespace.contains(".-")
                || namespace.contains("-.")
                || namespace.contains("google")
                || namespace.contains("g00gle")) {
            throw new IllegalArgumentException(
                    String.format("Invalid namespace name \"%s\"", namespace));
        }
    }

    /** Creates a CatalogDb instance with H2 as in-memory backend for unit tests. */
    static CatalogDb createInMemoryCatalogDb() {
        CatalogDb db = CatalogDb.createCatalogDb(CatalogDb.DbType.h2, null, null, "test");
        DSLContext ctx = db.getContext();
        ctx.transaction(
                c -> {
                    DSL.using(c).createSchemaIfNotExists(TABLES.getSchema()).execute();
                    DSL.using(c)
                            .createTableIfNotExists(TABLES)
                            .columns(TABLES.fields())
                            .constraint(primaryKey(TABLES.NAMESPACE_NAME, TABLES.TABLE_NAME))
                            .execute();
                    DSL.using(c).createSchemaIfNotExists(NAMESPACES.getSchema()).execute();
                    DSL.using(c)
                            .createTableIfNotExists(NAMESPACES)
                            .columns(NAMESPACES.fields())
                            .constraint(primaryKey(NAMESPACES.NAMESPACE_NAME))
                            .execute();
                });
        return db;
    }

    static Map<String, String> convertJSONStringToMap(String jsonString) {
        // convert to
        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, String>>() {}.getType(); // get K-V hashmap type
        return gson.fromJson(jsonString, type);
    }

    static String convertMapToJSONString(Map<String, String> map) {
        Gson gson = new Gson();
        return gson.toJson(map);
    }
}
