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
package org.apache.iceberg.snowflake;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.snowflake.entities.SnowflakeIdentifier;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

@RunWith(MockitoJUnitRunner.class)
public class JdbcSnowflakeClientTest {
  @Mock private Connection mockConnection;
  @Mock private JdbcClientPool mockClientPool;
  @Mock private QueryRunner mockQueryRunner;
  @Mock private ResultSet mockResultSet;

  private JdbcSnowflakeClient snowflakeClient;

  @Before
  public void before() throws SQLException, InterruptedException {
    snowflakeClient = new JdbcSnowflakeClient(mockClientPool);
    snowflakeClient.setQueryRunner(mockQueryRunner);

    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                return ((ClientPool.Action) invocation.getArguments()[0]).run(mockConnection);
              }
            })
        .when(mockClientPool)
        .run(any(ClientPool.Action.class));
    doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                return ((ResultSetHandler) invocation.getArguments()[2]).handle(mockResultSet);
              }
            })
        .when(mockQueryRunner)
        .query(
            any(Connection.class),
            any(String.class),
            any(ResultSetHandler.class),
            ArgumentMatchers.<Object>any());
  }

  @Test
  public void testNullClientPoolInConstructor() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new JdbcSnowflakeClient(null))
        .withMessageContaining("JdbcClientPool must be non-null");
  }

  /**
   * For the root scope, expect an underlying query to list schemas at the ACCOUNT level with no
   * query parameters.
   */
  @Test
  public void testListSchemasInAccount() throws SQLException {
    when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString("database_name"))
        .thenReturn("DB_1")
        .thenReturn("DB_1")
        .thenReturn("DB_2");
    when(mockResultSet.getString("name"))
        .thenReturn("SCHEMA_1")
        .thenReturn("SCHEMA_2")
        .thenReturn("SCHEMA_3");

    List<SnowflakeIdentifier> actualList =
        snowflakeClient.listSchemas(SnowflakeIdentifier.ofRoot());

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SHOW SCHEMAS IN ACCOUNT"),
            any(ResultSetHandler.class),
            eq((Object[]) null));

    Assertions.assertThat(actualList)
        .containsExactly(
            SnowflakeIdentifier.ofSchema("DB_1", "SCHEMA_1"),
            SnowflakeIdentifier.ofSchema("DB_1", "SCHEMA_2"),
            SnowflakeIdentifier.ofSchema("DB_2", "SCHEMA_3"));
  }

  /**
   * For a DATABASE scope, expect an underlying query to list schemas at the DATABASE level and
   * supply the database as a query param in an IDENTIFIER.
   */
  @Test
  public void testListSchemasInDatabase() throws SQLException {
    when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString("database_name")).thenReturn("DB_1").thenReturn("DB_1");
    when(mockResultSet.getString("name")).thenReturn("SCHEMA_1").thenReturn("SCHEMA_2");

    List<SnowflakeIdentifier> actualList =
        snowflakeClient.listSchemas(SnowflakeIdentifier.ofDatabase("DB_1"));

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SHOW SCHEMAS IN DATABASE IDENTIFIER(?)"),
            any(ResultSetHandler.class),
            eq("DB_1"));

    Assertions.assertThat(actualList)
        .containsExactly(
            SnowflakeIdentifier.ofSchema("DB_1", "SCHEMA_1"),
            SnowflakeIdentifier.ofSchema("DB_1", "SCHEMA_2"));
  }

  /**
   * Any unexpected SQLException from the underlying connection will propagate out as an
   * UncheckedSQLException when listing schemas.
   */
  @Test
  public void testListSchemasSQLException() throws SQLException, InterruptedException {
    when(mockClientPool.run(any(ClientPool.Action.class)))
        .thenThrow(new SQLException("Fake SQL exception"));
    Assert.assertThrows(
        UncheckedSQLException.class,
        () -> snowflakeClient.listSchemas(SnowflakeIdentifier.ofDatabase("DB_1")));
  }

  /**
   * Any unexpected InterruptedException from the underlying connection will propagate out as an
   * UncheckedInterruptedException when listing schemas.
   */
  @Test
  public void testListSchemasInterruptedException() throws SQLException, InterruptedException {
    when(mockClientPool.run(any(ClientPool.Action.class)))
        .thenThrow(new InterruptedException("Fake interrupted exception"));
    Assert.assertThrows(
        UncheckedInterruptedException.class,
        () -> snowflakeClient.listSchemas(SnowflakeIdentifier.ofDatabase("DB_1")));
  }

  /**
   * For the root/empty scope, expect an underlying query to list tables at the ACCOUNT level with
   * no query parameters.
   */
  @Test
  public void testListIcebergTablesInAccount() throws SQLException {
    when(mockResultSet.next())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(mockResultSet.getString("database_name"))
        .thenReturn("DB_1")
        .thenReturn("DB_1")
        .thenReturn("DB_1")
        .thenReturn("DB_2");
    when(mockResultSet.getString("schema_name"))
        .thenReturn("SCHEMA_1")
        .thenReturn("SCHEMA_1")
        .thenReturn("SCHEMA_2")
        .thenReturn("SCHEMA_3");
    when(mockResultSet.getString("name"))
        .thenReturn("TABLE_1")
        .thenReturn("TABLE_2")
        .thenReturn("TABLE_3")
        .thenReturn("TABLE_4");

    List<SnowflakeIdentifier> actualList =
        snowflakeClient.listIcebergTables(SnowflakeIdentifier.ofRoot());

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SHOW ICEBERG TABLES IN ACCOUNT"),
            any(ResultSetHandler.class),
            eq((Object[]) null));

    Assertions.assertThat(actualList)
        .containsExactly(
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1"),
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_2"),
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_2", "TABLE_3"),
            SnowflakeIdentifier.ofTable("DB_2", "SCHEMA_3", "TABLE_4"));
  }

  /**
   * For a DATABASE scope, expect an underlying query to list tables at the DATABASE level and
   * supply the database as a query param in an IDENTIFIER.
   */
  @Test
  public void testListIcebergTablesInDatabase() throws SQLException {
    when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString("database_name"))
        .thenReturn("DB_1")
        .thenReturn("DB_1")
        .thenReturn("DB_1");
    when(mockResultSet.getString("schema_name"))
        .thenReturn("SCHEMA_1")
        .thenReturn("SCHEMA_1")
        .thenReturn("SCHEMA_2");
    when(mockResultSet.getString("name"))
        .thenReturn("TABLE_1")
        .thenReturn("TABLE_2")
        .thenReturn("TABLE_3");

    List<SnowflakeIdentifier> actualList =
        snowflakeClient.listIcebergTables(SnowflakeIdentifier.ofDatabase("DB_1"));

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SHOW ICEBERG TABLES IN DATABASE IDENTIFIER(?)"),
            any(ResultSetHandler.class),
            eq("DB_1"));

    Assertions.assertThat(actualList)
        .containsExactly(
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1"),
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_2"),
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_2", "TABLE_3"));
  }

  /**
   * For a SCHEMA scope, expect an underlying query to list tables at the SCHEMA level and supply
   * the schema as a query param in an IDENTIFIER.
   */
  @Test
  public void testListIcebergTablesInSchema() throws SQLException {
    when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString("database_name")).thenReturn("DB_1").thenReturn("DB_1");
    when(mockResultSet.getString("schema_name")).thenReturn("SCHEMA_1").thenReturn("SCHEMA_1");
    when(mockResultSet.getString("name")).thenReturn("TABLE_1").thenReturn("TABLE_2");

    List<SnowflakeIdentifier> actualList =
        snowflakeClient.listIcebergTables(SnowflakeIdentifier.ofSchema("DB_1", "SCHEMA_1"));

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SHOW ICEBERG TABLES IN SCHEMA IDENTIFIER(?)"),
            any(ResultSetHandler.class),
            eq("DB_1.SCHEMA_1"));

    Assertions.assertThat(actualList)
        .containsExactly(
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1"),
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_2"));
  }

  /**
   * Any unexpected SQLException from the underlying connection will propagate out as an
   * UncheckedSQLException when listing tables.
   */
  @Test
  public void testListIcebergTablesSQLException() throws SQLException, InterruptedException {
    when(mockClientPool.run(any(ClientPool.Action.class)))
        .thenThrow(new SQLException("Fake SQL exception"));
    Assert.assertThrows(
        UncheckedSQLException.class,
        () -> snowflakeClient.listIcebergTables(SnowflakeIdentifier.ofDatabase("DB_1")));
  }

  /**
   * Any unexpected InterruptedException from the underlying connection will propagate out as an
   * UncheckedInterruptedException when listing tables.
   */
  @Test
  public void testListIcebergTablesInterruptedException()
      throws SQLException, InterruptedException {
    when(mockClientPool.run(any(ClientPool.Action.class)))
        .thenThrow(new InterruptedException("Fake interrupted exception"));
    Assert.assertThrows(
        UncheckedInterruptedException.class,
        () -> snowflakeClient.listIcebergTables(SnowflakeIdentifier.ofDatabase("DB_1")));
  }

  /**
   * Test parsing of table metadata JSON from a GET_ICEBERG_TABLE_INFORMATION call, with the S3 path
   * unaltered between snowflake/iceberg path representations.
   */
  @Test
  public void testGetS3TableMetadata() throws SQLException {
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString("METADATA"))
        .thenReturn(
            "{\"metadataLocation\":\"s3://tab1/metadata/v3.metadata.json\",\"status\":\"success\"}");

    SnowflakeTableMetadata actualMetadata =
        snowflakeClient.getTableMetadata(
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1"));

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) AS METADATA"),
            any(ResultSetHandler.class),
            eq("DB_1.SCHEMA_1.TABLE_1"));

    SnowflakeTableMetadata expectedMetadata =
        new SnowflakeTableMetadata(
            "s3://tab1/metadata/v3.metadata.json",
            "s3://tab1/metadata/v3.metadata.json",
            "success",
            null);
    Assert.assertEquals(expectedMetadata, actualMetadata);
  }

  /**
   * Test parsing of table metadata JSON from a GET_ICEBERG_TABLE_INFORMATION call, with the Azure
   * path translated from an azure:// format to a wasbs:// format.
   */
  @Test
  public void testGetAzureTableMetadata() throws SQLException {
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString("METADATA"))
        .thenReturn(
            "{\"metadataLocation\":\"azure://myaccount.blob.core.windows.net/mycontainer/tab3/metadata/v334.metadata.json\",\"status\":\"success\"}");

    SnowflakeTableMetadata actualMetadata =
        snowflakeClient.getTableMetadata(
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1"));

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) AS METADATA"),
            any(ResultSetHandler.class),
            eq("DB_1.SCHEMA_1.TABLE_1"));

    SnowflakeTableMetadata expectedMetadata =
        new SnowflakeTableMetadata(
            "azure://myaccount.blob.core.windows.net/mycontainer/tab3/metadata/v334.metadata.json",
            "wasbs://mycontainer@myaccount.blob.core.windows.net/tab3/metadata/v334.metadata.json",
            "success",
            null);
    Assert.assertEquals(expectedMetadata, actualMetadata);
  }

  /**
   * Test parsing of table metadata JSON from a GET_ICEBERG_TABLE_INFORMATION call, with the GCS
   * path translated from a gcs:// format to a gs:// format.
   */
  @Test
  public void testGetGcsTableMetadata() throws SQLException {
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString("METADATA"))
        .thenReturn(
            "{\"metadataLocation\":\"gcs://tab5/metadata/v793.metadata.json\",\"status\":\"success\"}");

    SnowflakeTableMetadata actualMetadata =
        snowflakeClient.getTableMetadata(
            SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1"));

    verify(mockQueryRunner)
        .query(
            eq(mockConnection),
            eq("SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) AS METADATA"),
            any(ResultSetHandler.class),
            eq("DB_1.SCHEMA_1.TABLE_1"));

    SnowflakeTableMetadata expectedMetadata =
        new SnowflakeTableMetadata(
            "gcs://tab5/metadata/v793.metadata.json",
            "gs://tab5/metadata/v793.metadata.json",
            "success",
            null);
    Assert.assertEquals(expectedMetadata, actualMetadata);
  }

  /** Malformed JSON from a ResultSet should propagate as an IllegalArgumentException. */
  @Test
  public void testGetTableMetadataMalformedJson() throws SQLException {
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString("METADATA")).thenReturn("{\"malformed_no_closing_bracket");
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            snowflakeClient.getTableMetadata(
                SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1")));
  }

  /**
   * Any unexpected SQLException from the underlying connection will propagate out as an
   * UncheckedSQLException when getting table metadata.
   */
  @Test
  public void testGetTableMetadataSQLException() throws SQLException, InterruptedException {
    when(mockClientPool.run(any(ClientPool.Action.class)))
        .thenThrow(new SQLException("Fake SQL exception"));
    Assert.assertThrows(
        UncheckedSQLException.class,
        () ->
            snowflakeClient.getTableMetadata(
                SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1")));
  }

  /**
   * Any unexpected InterruptedException from the underlying connection will propagate out as an
   * UncheckedInterruptedException when getting table metadata.
   */
  @Test
  public void testGetTableMetadataInterruptedException() throws SQLException, InterruptedException {
    when(mockClientPool.run(any(ClientPool.Action.class)))
        .thenThrow(new InterruptedException("Fake interrupted exception"));
    Assert.assertThrows(
        UncheckedInterruptedException.class,
        () ->
            snowflakeClient.getTableMetadata(
                SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TABLE_1")));
  }

  /** Calling close() propagates to closing underlying client pool. */
  @Test
  public void testClose() {
    snowflakeClient.close();
    verify(mockClientPool).close();
  }
}
