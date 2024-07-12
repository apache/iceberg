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
package org.apache.iceberg.lock;

import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class TestAuthImpl implements HttpAuthentication {

  private long timeStamp = -1L;

  @Override
  public Map<String, String> assignRequestHeader(String httpUrl) {
    return ImmutableMap.of("timeStamp", String.valueOf(timeStamp));
  }

  @Override
  public boolean confirmResponse(String respBody) {
    return true;
  }

  @Override
  public void init(Map<String, String> props) {
    timeStamp = MapUtils.getLongValue(props, "timeStamp", System.currentTimeMillis());
  }
}
