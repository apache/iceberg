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
package org.apache.iceberg.jdbc;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestJdbcTableConcurrency {

  static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "test_table");
  static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  @Test
  public synchronized void testConcurrentFastAppends() throws IOException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, tableDir.getAbsolutePath());
    String sqliteDb = "jdbc:sqlite:" + tableDir.getAbsolutePath() + "concurentFastAppend.db";
    properties.put(CatalogProperties.URI, sqliteDb);
    JdbcCatalog catalog = new JdbcCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("jdbc", properties);
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);

    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    String fileName = UUID.randomUUID().toString();
    DataFile file =
        DataFiles.builder(icebergTable.spec())
            .withPath(FileFormat.PARQUET.addExtension(fileName))
            .withRecordCount(2)
            .withFileSizeInBytes(0)
            .build();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);
    int threadsCount = 2;
    Tasks.range(threadsCount)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            index -> {
              for (int numCommittedFiles = 0; numCommittedFiles < 10; numCommittedFiles++) {
                final int currentFilesCount = numCommittedFiles;
                Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .atMost(Duration.ofSeconds(10))
                    .until(() -> barrier.get() >= currentFilesCount * threadsCount);
                icebergTable.newFastAppend().appendFile(file).commit();
                barrier.incrementAndGet();
              }
            });

    icebergTable.refresh();
    assertThat(icebergTable.currentSnapshot().allManifests(icebergTable.io())).hasSize(20);
  }

  @Test
  public synchronized void testConcurrentConnections() throws InterruptedException, IOException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, tableDir.getAbsolutePath());
    String sqliteDb = "jdbc:sqlite:" + tableDir.getAbsolutePath() + "concurentConnections.db";
    properties.put(CatalogProperties.URI, sqliteDb);
    JdbcCatalog catalog = new JdbcCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("jdbc", properties);
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);

    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    icebergTable
        .updateProperties()
        .set(COMMIT_NUM_RETRIES, "20")
        .set(COMMIT_MIN_RETRY_WAIT_MS, "25")
        .set(COMMIT_MAX_RETRY_WAIT_MS, "25")
        .commit();

    String fileName = UUID.randomUUID().toString();
    DataFile file =
        DataFiles.builder(icebergTable.spec())
            .withPath(FileFormat.PARQUET.addExtension(fileName))
            .withRecordCount(2)
            .withFileSizeInBytes(0)
            .build();

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(7));

    for (int i = 0; i < 7; i++) {
      executorService.submit(() -> icebergTable.newAppend().appendFile(file).commit());
    }

    executorService.shutdown();
    assertThat(executorService.awaitTermination(3, TimeUnit.MINUTES)).as("Timeout").isTrue();
    assertThat(Iterables.size(icebergTable.snapshots())).isEqualTo(7);
  }

  @Test
  public synchronized void testInitializeWithSlowConcurrentConnections()
      throws InterruptedException, SQLException, ExecutionException, ClassNotFoundException {
    // number of threads and requests to attempt.
    int parallelism = 2;
    // verifies that multiple calls to initialize with slow responses will not fail.
    Map<String, String> properties = Maps.newHashMap();

    properties.put(CatalogProperties.WAREHOUSE_LOCATION, tableDir.getAbsolutePath());
    String testingDB = "jdbc:slow:derby:memory:testDb;create=true";
    new org.apache.derby.jdbc.EmbeddedDriver();
    properties.put(CatalogProperties.URI, testingDB);
    SlowDriver slowDriver = new SlowDriver(testingDB);

    Callable<JdbcCatalog> makeCatalog =
        () -> {
          JdbcCatalog catalog = new JdbcCatalog();
          catalog.setConf(new Configuration());
          catalog.initialize("jdbc", properties);
          return catalog;
        };

    try {
      DriverManager.registerDriver(slowDriver);
      ExecutorService executorService =
          MoreExecutors.getExitingExecutorService(
              (ThreadPoolExecutor) Executors.newFixedThreadPool(parallelism));

      List<Future<JdbcCatalog>> futures = Lists.newArrayList();
      for (int i = 0; i < parallelism; i++) {
        futures.add(executorService.submit(makeCatalog));
      }
      for (Future<JdbcCatalog> future : futures) {
        future.get();
      }
    } finally {
      DriverManager.deregisterDriver(slowDriver);
    }
  }

  /** A Connection implementation that returns SlowPreparedStatements */
  private static class SlowJDBCConnection implements Connection {
    Connection delegate;

    SlowJDBCConnection(Connection delegate) {
      this.delegate = delegate;
    }

    @Override
    public Statement createStatement() throws SQLException {
      return delegate.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
      return new SlowPreparedStatement(delegate.prepareStatement(sql));
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
      return delegate.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
      return delegate.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
      delegate.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
      return delegate.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
      delegate.commit();
    }

    @Override
    public void rollback() throws SQLException {
      delegate.rollback();
    }

    @Override
    public void close() throws SQLException {
      delegate.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
      return delegate.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
      return delegate.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
      delegate.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
      return delegate.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
      delegate.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
      return delegate.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
      delegate.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
      return delegate.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
      delegate.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
        throws SQLException {
      return delegate.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
      return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
      return delegate.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
      delegate.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
      delegate.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
      return delegate.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
      return delegate.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
      return delegate.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
      delegate.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
      delegate.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(
        int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(
        String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
      return delegate.prepareStatement(
          sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(
        String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
      return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
        throws SQLException {
      return delegate.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      return delegate.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
        throws SQLException {
      return delegate.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
      return delegate.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
      return delegate.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
      return delegate.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
      return delegate.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
      return delegate.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
      delegate.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
      delegate.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
      return delegate.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
      return delegate.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return delegate.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return delegate.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
      delegate.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
      return delegate.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
      delegate.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      delegate.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
      return delegate.getNetworkTimeout();
    }

    @Override
    public void beginRequest() throws SQLException {
      delegate.beginRequest();
    }

    @Override
    public void endRequest() throws SQLException {
      delegate.endRequest();
    }

    @Override
    public boolean setShardingKeyIfValid(
        ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
      return delegate.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
    }

    @Override
    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
      return delegate.setShardingKeyIfValid(shardingKey, timeout);
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey)
        throws SQLException {
      delegate.setShardingKey(shardingKey, superShardingKey);
    }

    @Override
    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
      delegate.setShardingKey(shardingKey);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return delegate.isWrapperFor(iface);
    }
  }

  /** A slow prepared statement that has a 500 ms delay before evaluating the execute() method. */
  private static class SlowPreparedStatement implements PreparedStatement {
    private final PreparedStatement delegate;

    SlowPreparedStatement(PreparedStatement delegate) {
      this.delegate = delegate;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
      return delegate.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
      return delegate.executeUpdate();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
      delegate.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
      delegate.setBoolean(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
      delegate.setByte(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
      delegate.setShort(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
      delegate.setInt(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
      delegate.setLong(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
      delegate.setFloat(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
      delegate.setDouble(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
      delegate.setBigDecimal(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
      delegate.setString(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
      delegate.setBytes(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
      delegate.setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
      delegate.setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
      delegate.setTimestamp(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
      delegate.setAsciiStream(parameterIndex, x, length);
    }

    @Deprecated(since = "1.2")
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length)
        throws SQLException {
      delegate.setUnicodeStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
      delegate.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
      delegate.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
      delegate.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
      delegate.setObject(parameterIndex, x);
    }

    @Override
    public boolean execute() throws SQLException {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return delegate.execute();
    }

    @Override
    public void addBatch() throws SQLException {
      delegate.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
        throws SQLException {
      delegate.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
      delegate.setRef(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
      delegate.setBlob(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
      delegate.setClob(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
      delegate.setArray(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
      return delegate.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
      delegate.setDate(parameterIndex, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
      delegate.setTime(parameterIndex, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
      delegate.setTimestamp(parameterIndex, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
      delegate.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
      delegate.setURL(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
      return delegate.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
      delegate.setRowId(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
      delegate.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
        throws SQLException {
      delegate.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
      delegate.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
      delegate.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
        throws SQLException {
      delegate.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
      delegate.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
      delegate.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
        throws SQLException {
      delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
      delegate.setAsciiStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
        throws SQLException {
      delegate.setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
        throws SQLException {
      delegate.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
      delegate.setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
      delegate.setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
      delegate.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
      delegate.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
      delegate.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
      delegate.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
      delegate.setNClob(parameterIndex, reader);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)
        throws SQLException {
      delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
      delegate.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
      return delegate.executeLargeUpdate();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
      return delegate.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
      return delegate.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
      delegate.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
      return delegate.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
      delegate.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
      return delegate.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
      delegate.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
      delegate.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
      return delegate.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
      delegate.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
      delegate.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      return delegate.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
      delegate.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
      delegate.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
      return delegate.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
      return delegate.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
      return delegate.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
      return delegate.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
      delegate.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
      return delegate.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
      delegate.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
      return delegate.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
      return delegate.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
      return delegate.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
      delegate.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
      delegate.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
      return delegate.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
      return delegate.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
      return delegate.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
      return delegate.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      return delegate.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
      return delegate.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
      return delegate.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
      return delegate.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
      return delegate.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
      return delegate.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
      return delegate.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
      return delegate.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
      delegate.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
      return delegate.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
      delegate.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
      return delegate.isCloseOnCompletion();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
      return delegate.getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
      delegate.setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
      return delegate.getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
      return delegate.executeLargeBatch();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
      return delegate.executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      return delegate.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
      return delegate.executeLargeUpdate(sql, columnIndexes);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
      return delegate.executeLargeUpdate(sql, columnNames);
    }

    @Override
    public String enquoteLiteral(String val) throws SQLException {
      return delegate.enquoteLiteral(val);
    }

    @Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
      return delegate.enquoteIdentifier(identifier, alwaysQuote);
    }

    @Override
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
      return delegate.isSimpleIdentifier(identifier);
    }

    @Override
    public String enquoteNCharLiteral(String val) throws SQLException {
      return delegate.enquoteNCharLiteral(val);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return delegate.isWrapperFor(iface);
    }
  }

  /**
   * A driver that wraps a true driver implementation and returns SlopPreparedStatements. URL for
   * this driver is "jdbc:slow:true_driver_url":
   */
  private static class SlowDriver implements Driver {
    private static final String PREFIX = "jdbc:slow:";

    private Driver delegate;

    SlowDriver(String url) throws SQLException {
      if (!url.startsWith(PREFIX)) {
        throw new SQLException("url must start with " + PREFIX);
      }
      delegate = DriverManager.getDriver(rewriteUrl(url));
    }

    static String rewriteUrl(String url) {
      return url.startsWith(PREFIX) ? "jdbc:" + url.substring(PREFIX.length()) : url;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
      return new SlowJDBCConnection(delegate.connect(rewriteUrl(url), info));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
      return url.startsWith(PREFIX) && delegate.acceptsURL(rewriteUrl(url));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
      return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
      return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
      return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
      return delegate.jdbcCompliant();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }
  }
}
