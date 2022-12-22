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
package org.apache.iceberg.encryption;

import java.util.Map;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class KmsUtil {

  private KmsUtil() {}

  public static KeyManagementClient loadKmsClient(
      String classPath, Map<String, String> properties) {
    Preconditions.checkNotNull(classPath, "Cannot initialize KmsClient, class name is null");

    DynConstructors.Ctor<KeyManagementClient> ctor;
    try {
      ctor = DynConstructors.builder(KeyManagementClient.class).impl(classPath).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize KeyManagementClient, missing no-arg constructor for class %s",
              classPath),
          e);
    }

    KeyManagementClient kmsClient;
    try {
      kmsClient = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize kms client, %s does not implement KeyManagementClient interface",
              classPath),
          e);
    }

    kmsClient.initialize(properties);
    return kmsClient;
  }
}
