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
package org.apache.iceberg.gcp.biglake;

import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.protobuf.AbstractMessage;
import io.grpc.ServerServiceDefinition;
import java.util.ArrayList;
import java.util.List;

/** Mock the BigLake Metastore service for testing. */
public class MockMetastoreService implements MockGrpcService {

  private final MockMetastoreServiceImpl serviceImpl;

  public MockMetastoreService() {
    serviceImpl = new MockMetastoreServiceImpl();
  }

  @Override
  public List<AbstractMessage> getRequests() {
    return new ArrayList<AbstractMessage>();
  }

  @Override
  public void addResponse(AbstractMessage response) {}

  @Override
  public void addException(Exception exception) {}

  @Override
  public ServerServiceDefinition getServiceDefinition() {
    return serviceImpl.bindService();
  }

  @Override
  public void reset() {
    serviceImpl.reset();
  }
}
