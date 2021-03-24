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

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestClientPoolImpl {

  HiveClientPool clients;

  @Before
  public void before() {
    HiveClientPool clientPool = new HiveClientPool(2, new Configuration());
    clients = Mockito.spy(clientPool);
  }

  @After
  public void after() {
    clients.close();
    clients = null;
  }

  @Test
  public void testNewClientFailure() {
    Mockito.doThrow(new RuntimeException("Connection exception")).when(clients).newClient();
    AssertHelpers.assertThrows("Should throw exception", RuntimeException.class,
        "Connection exception", () -> clients.run(Object::toString));
  }

  @Test
  public void testGetTablesFailsForNonReconnectableException() throws Exception {
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(hmsClient).when(clients).newClient();
    Mockito.doThrow(new MetaException("Another meta exception"))
      .when(hmsClient).getTables(Mockito.anyString(), Mockito.anyString());
    AssertHelpers.assertThrows("Should throw exception", MetaException.class,
        "Another meta exception", () -> clients.run(client -> client.getTables("default", "t")));
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
    Assert.assertEquals(databases, clients.run(client -> client.getAllDatabases()));

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
        new Function("concat", "db1", "classname", "root", PrincipalType.USER, 100, FunctionType.JAVA, null));
    Mockito.doReturn(response).when(newClient).getAllFunctions();

    Assert.assertEquals(response, clients.run(client -> client.getAllFunctions()));

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
