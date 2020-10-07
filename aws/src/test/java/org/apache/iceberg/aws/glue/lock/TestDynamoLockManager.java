/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.aws.glue.lock;

import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.iceberg.aws.glue.util.AWSGlueConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestDynamoLockManager {

  private static final String DYNAMO_LOCAL_DOWNLOAD_URL =
          "https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz";
  private static final String DYNAMO_LOCAL_DIR_NAME = System.getProperty("java.io.tmpdir") + "dynamo-local";
  private static final int DYNAMO_LOCAL_SERVER_PORT = 8001;

  private Process dynamoLocalProcess;
  private DynamoDbClient dynamo;
  private LockManager lockManager;

  /**
   * We download the DynamoDB executable jar to local and run the server for testing.
   * The jar certificate provided by maven central conflicts with iceberg and needs to be fixed.
   * @throws Exception exception
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    Runtime rt = Runtime.getRuntime();
    Process pr = rt.exec(String.format("curl %s -o %s.tar.gz", DYNAMO_LOCAL_DOWNLOAD_URL, DYNAMO_LOCAL_DIR_NAME));
    pr.waitFor();
    pr = rt.exec(String.format("mkdir %s", DYNAMO_LOCAL_DIR_NAME));
    pr.waitFor();
    pr = rt.exec(String.format("tar xvzf %s.tar.gz -C %s", DYNAMO_LOCAL_DIR_NAME, DYNAMO_LOCAL_DIR_NAME));
    pr.waitFor();
  }

  @Before
  public void before() throws Exception {
    Runtime rt = Runtime.getRuntime();
    dynamoLocalProcess = rt.exec(String.format("java -Djava.library.path=%s/DynamoDBLocal_lib -jar " +
            "%s/DynamoDBLocal.jar -inMemory -port %s",
            DYNAMO_LOCAL_DIR_NAME, DYNAMO_LOCAL_DIR_NAME, DYNAMO_LOCAL_SERVER_PORT));
    Configuration configuration = new Configuration();
    dynamo = DynamoDbClient.builder()
            .endpointOverride(URI.create(String.format("http://localhost:%s", DYNAMO_LOCAL_SERVER_PORT)))
            .region(Region.US_EAST_1) // dummy region
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("key", "secret"))) // dummy credential
            .build();
    lockManager = new DynamoLockManager(configuration, dynamo);
  }

  @After
  public void after() throws Exception {
    dynamo.deleteTable(DeleteTableRequest.builder()
            .tableName(AWSGlueConfig.AWS_GLUE_LOCK_COMPONENT_DYNAMO_TABLE_NAME_DEFAULT)
            .build());
    dynamo.deleteTable(DeleteTableRequest.builder()
            .tableName(AWSGlueConfig.AWS_GLUE_LOCK_REQUEST_DYNAMO_TABLE_NAME_DEFAULT)
            .build());
    dynamoLocalProcess.destroy();
    dynamoLocalProcess.waitFor();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    Runtime.getRuntime().exec(String.format("rm %s.tar.gz", DYNAMO_LOCAL_DIR_NAME)).waitFor();
    Runtime.getRuntime().exec(String.format("rm -rf %s", DYNAMO_LOCAL_DIR_NAME)).waitFor();
  }

  @Test
  public void testTableCreation() {
    // if fail, will throw ResourceNotFoundException
    dynamo.describeTable(DescribeTableRequest.builder()
            .tableName(AWSGlueConfig.AWS_GLUE_LOCK_COMPONENT_DYNAMO_TABLE_NAME_DEFAULT)
            .build());
    dynamo.describeTable(DescribeTableRequest.builder()
            .tableName(AWSGlueConfig.AWS_GLUE_LOCK_REQUEST_DYNAMO_TABLE_NAME_DEFAULT)
            .build());
  }

  @Test
  public void testLock_singleLock_singleProcess() throws Exception {
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("table1");
    LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    LockResponse response = lockManager.lock(lockRequest);
    assertEquals(LockState.ACQUIRED, response.getState());
    // check lock again should still be acquired
    response = lockManager.checkLock(response.getLockid());
    assertEquals(LockState.ACQUIRED, response.getState());
  }

  @Test
  public void testLock_singleLock_sequentialProcesses() throws Exception {
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("table1");
    LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    LockResponse response = lockManager.lock(lockRequest);
    assertEquals(LockState.ACQUIRED, response.getState());
    LockResponse response2 = lockManager.lock(lockRequest);
    assertNotEquals(response2.getLockid(), response.getLockid());
    assertEquals(LockState.WAITING, response2.getState());
  }

  @Test
  public void testLock_singleLock_parallelProcesses() throws Exception {
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("table1");
    LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    List<LockResponse> responses = IntStream.range(0, 100).parallel()
            .mapToObj(i -> lockManager.lock(lockRequest))
            .collect(Collectors.toList());
    assertEquals(1, responses.stream()
            .map(LockResponse::getState)
            .filter(s -> s.equals(LockState.ACQUIRED))
            .count());
  }

  @Test
  public void testLock_multipleLocks_sequentialProcesses() throws Exception {
    List<LockComponent> components = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db" + i);
      lockComponent.setTablename("table" + i);
      components.add(lockComponent);
    }
    LockRequest lockRequest = new LockRequest(components,
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    LockResponse response = lockManager.lock(lockRequest);
    assertEquals(LockState.ACQUIRED, response.getState());
    LockResponse response2 = lockManager.lock(lockRequest);
    assertNotEquals(response2.getLockid(), response.getLockid());
    assertEquals(LockState.WAITING, response2.getState());
  }

  @Test
  public void testLock_multipleLock_parallelProcesses() throws Exception {
    List<LockComponent> components = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db" + i);
      lockComponent.setTablename("table" + i);
      components.add(lockComponent);
    }
    LockRequest lockRequest = new LockRequest(components,
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    List<LockResponse> responses = IntStream.range(0, 100).parallel()
            .mapToObj(i -> lockManager.lock(lockRequest))
            .collect(Collectors.toList());
    assertEquals(1, responses.stream()
            .map(LockResponse::getState)
            .filter(s -> s.equals(LockState.ACQUIRED))
            .count());
  }

  @Test
  public void testLock_lockExpire() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AWSGlueConfig.AWS_GLUE_LOCK_TIMEOUT_MILLIS, "2000");
    lockManager = new DynamoLockManager(conf, dynamo);
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("table1");
    LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    LockResponse response = lockManager.lock(lockRequest);
    assertEquals(LockState.ACQUIRED, response.getState());
    Thread.sleep(2000);
    response = lockManager.checkLock(response.getLockid());
    assertEquals(LockState.NOT_ACQUIRED, response.getState());
    response = lockManager.lock(lockRequest);
    assertEquals(LockState.ACQUIRED, response.getState());
  }

  @Test
  public void testUnlock() throws Exception {
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, "db");
    lockComponent.setTablename("table1");
    LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
            System.getProperty("user.name"),
            InetAddress.getLocalHost().getHostName());
    LockResponse response = lockManager.lock(lockRequest);
    assertEquals(LockState.ACQUIRED, response.getState());
    lockManager.unlock(response.getLockid());
    // check again it should be not acquired
    response = lockManager.checkLock(response.getLockid());
    assertEquals(LockState.NOT_ACQUIRED, response.getState());
    response = lockManager.lock(lockRequest);
    assertEquals(LockState.ACQUIRED, response.getState());
  }
}
