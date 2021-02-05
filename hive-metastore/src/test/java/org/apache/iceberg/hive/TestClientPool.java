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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestClientPool {

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

  @Test(expected = RuntimeException.class)
  public void testNewClientFailure() throws Exception {
    try (MockClientPool pool = new MockClientPool(2, Exception.class)) {
      pool.run(Object::toString);
    }
  }

  @Test
  public void testConnectionFailureRestore() throws Exception {

    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(hmsClient).when(clients).newClient();

    // Throwing an exception may trigger the client to reconnect.
    String metaMessage = "Got exception: org.apache.thrift.transport.TTransportException";
    Mockito.doThrow(new MetaException(metaMessage)).when(hmsClient).getAllDatabases();
    Mockito.doThrow(new MetaException(metaMessage)).when(hmsClient).getAllFunctions();

    // Create a new client when the reconnect method is called.
    HiveMetaStoreClient newClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(newClient).when(clients).reconnect(hmsClient);

    Mockito.doReturn(Lists.newArrayList("db1", "db2")).when(newClient).getAllDatabases();
    GetAllFunctionsResponse response = new GetAllFunctionsResponse();
    response.addToFunctions(
        new Function("concat", "db1", "classname", "root", PrincipalType.USER, 100, FunctionType.JAVA, null));
    Mockito.doReturn(response).when(newClient).getAllFunctions();
    // The return is OK when the reconnect method is called.
    Assert.assertEquals(Lists.newArrayList("db1", "db2"),
        clients.run(client -> client.getAllDatabases()));

    // Verify that the method is called.
    Mockito.verify(clients).reconnect(hmsClient);

    // The return is OK, because the client pool uses a new client.
    Assert.assertEquals(response, clients.run(client -> client.getAllFunctions()));

    Mockito.verify(clients, Mockito.times(1)).reconnect(hmsClient);
  }

  @Test(expected = MetaException.class)
  public void testConnectionFailure() throws Exception {
    HiveMetaStoreClient hmsClient = Mockito.mock(HiveMetaStoreClient.class);
    Mockito.doReturn(hmsClient).when(clients).newClient();
    Mockito.doThrow(new MetaException("Another meta exception"))
      .when(hmsClient).getTables(Mockito.anyString(), Mockito.anyString());
    clients.run(client -> client.getTables("default", "t"));
  }

  private static class MockClientPool extends ClientPool<Object, Exception> {

    MockClientPool(int poolSize, Class<? extends Exception> reconnectExc) {
      super(poolSize, reconnectExc);
    }

    @Override
    protected Object newClient() {
      throw new RuntimeException();
    }

    @Override
    protected Object reconnect(Object client) {
      return null;
    }

    @Override
    protected void close(Object client) {

    }
  }
}
