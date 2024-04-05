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
package org.apache.iceberg.hive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestHiveClientPool {

  private static final String HIVE_SITE_CONTENT =
      "<?xml version=\"1.0\"?>\n"
          + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n"
          + "<configuration>\n"
          + "  <property>\n"
          + "    <name>hive.metastore.sasl.enabled</name>\n"
          + "    <value>true</value>\n"
          + "  </property>\n"
          + "</configuration>\n";

  HiveClientPool clients;

  @BeforeEach
  public void before() {
    HiveClientPool clientPool = new HiveClientPool(2, new Configuration());
    clients = Mockito.spy(clientPool);
  }

  @AfterEach
  public void after() {
    clients.close();
    clients = null;
  }

  @Test
  public void testConf() {
    HiveConf conf = createHiveConf();
    conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:/mywarehouse/");

    HiveClientPool clientPool = new HiveClientPool(10, conf);
    HiveConf clientConf = clientPool.hiveConf();

    assertThat(clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname))
        .isEqualTo(conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
    assertThat(clientPool.poolSize()).isEqualTo(10);

    // 'hive.metastore.sasl.enabled' should be 'true' as defined in xml
    assertThat(clientConf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname))
        .isEqualTo(conf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));
    assertThat(clientConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL)).isTrue();
  }

  private HiveConf createHiveConf() {
    HiveConf hiveConf = new HiveConf();
    try (InputStream inputStream =
        new ByteArrayInputStream(HIVE_SITE_CONTENT.getBytes(StandardCharsets.UTF_8))) {
      hiveConf.addResource(inputStream, "for_test");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return hiveConf;
  }

  @Test
  public void testNewClientFailure() {
    Mockito.doThrow(new RuntimeException("Connection exception")).when(clients).newClient();
    assertThatThrownBy(() -> clients.run(Object::toString))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Connection exception");
  }

  @Test
  public void testGetTablesFailsForNonReconnectableException() throws Exception {
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(hmsClient).when(clients).newClient();
    Mockito.doThrow(new MetaException("Another meta exception"))
        .when(hmsClient)
        .getTables(Mockito.anyString(), Mockito.anyString());
    assertThatThrownBy(() -> clients.run(client -> client.getTables("default", "t")))
        .isInstanceOf(MetaException.class)
        .hasMessage("Another meta exception");
  }

  @Test
  public void testExceptionMessages() {
    try (MockedStatic<MetaStoreUtils> mockedStatic = Mockito.mockStatic(MetaStoreUtils.class)) {
      mockedStatic
          .when(() -> MetaStoreUtils.newInstance(any(), any(), any()))
          .thenThrow(new RuntimeException(new MetaException("Another meta exception")));
      assertThatThrownBy(() -> clients.run(client -> client.getTables("default", "t")))
          .isInstanceOf(RuntimeMetaException.class)
          .hasMessage("Failed to connect to Hive Metastore");
    }

    try (MockedStatic<MetaStoreUtils> mockedStatic = Mockito.mockStatic(MetaStoreUtils.class)) {
      mockedStatic
          .when(() -> MetaStoreUtils.newInstance(any(), any(), any()))
          .thenThrow(new RuntimeException(new MetaException()));
      assertThatThrownBy(() -> clients.run(client -> client.getTables("default", "t")))
          .isInstanceOf(RuntimeMetaException.class)
          .hasMessage("Failed to connect to Hive Metastore");
    }

    try (MockedStatic<MetaStoreUtils> mockedStatic = Mockito.mockStatic(MetaStoreUtils.class)) {
      mockedStatic
          .when(() -> MetaStoreUtils.newInstance(any(), any(), any()))
          .thenThrow(new RuntimeException());
      assertThatThrownBy(() -> clients.run(client -> client.getTables("default", "t")))
          .isInstanceOf(RuntimeMetaException.class)
          .hasMessage("Failed to connect to Hive Metastore");
    }

    try (MockedStatic<MetaStoreUtils> mockedStatic = Mockito.mockStatic(MetaStoreUtils.class)) {
      mockedStatic
          .when(() -> MetaStoreUtils.newInstance(any(), any(), any()))
          .thenThrow(new RuntimeException("Another instance of Derby may have already booted"));
      assertThatThrownBy(() -> clients.run(client -> client.getTables("default", "t")))
          .isInstanceOf(RuntimeMetaException.class)
          .hasMessage(
              "Failed to start an embedded metastore because embedded "
                  + "Derby supports only one client at a time. To fix this, use a metastore that supports "
                  + "multiple clients.");
    }
  }

  @Test
  public void testConnectionFailureRestoreForMetaException() throws Exception {
    HiveMetaStoreClient hmsClient = newClient();

    // Throwing an exception may trigger the client to reconnect.
    String metaMessage = "Got exception: org.apache.thrift.transport.TTransportException";
    Mockito.doThrow(new MetaException(metaMessage)).when(hmsClient).getAllDatabases();

    // Create a new client when the reconnect method is called.
    HiveMetaStoreClient newClient = reconnect(hmsClient);

    List<String> databases = Lists.newArrayList("db1", "db2");

    Mockito.doReturn(databases).when(newClient).getAllDatabases();
    // The return is OK when the reconnect method is called.
    assertThat((List<String>) clients.run(client -> client.getAllDatabases(), true))
        .isEqualTo(databases);

    // Verify that the method is called.
    Mockito.verify(clients).reconnect(hmsClient);
    Mockito.verify(clients, Mockito.never()).reconnect(newClient);
  }

  @Test
  public void testConnectionFailureRestoreForTTransportException() throws Exception {
    HiveMetaStoreClient hmsClient = newClient();
    Mockito.doThrow(new TTransportException()).when(hmsClient).getAllFunctions();

    // Create a new client when getAllFunctions() failed.
    HiveMetaStoreClient newClient = reconnect(hmsClient);

    GetAllFunctionsResponse response = new GetAllFunctionsResponse();
    response.addToFunctions(
        new Function(
            "concat",
            "db1",
            "classname",
            "root",
            PrincipalType.USER,
            100,
            FunctionType.JAVA,
            null));
    Mockito.doReturn(response).when(newClient).getAllFunctions();
    assertThat((GetAllFunctionsResponse) clients.run(client -> client.getAllFunctions(), true))
        .isEqualTo(response);

    Mockito.verify(clients).reconnect(hmsClient);
    Mockito.verify(clients, Mockito.never()).reconnect(newClient);
  }

  private HiveMetaStoreClient newClient() {
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(hmsClient).when(clients).newClient();
    return hmsClient;
  }

  private HiveMetaStoreClient reconnect(HiveMetaStoreClient obsoleteClient) {
    HiveMetaStoreClient newClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(newClient).when(clients).reconnect(obsoleteClient);
    return newClient;
  }
}
