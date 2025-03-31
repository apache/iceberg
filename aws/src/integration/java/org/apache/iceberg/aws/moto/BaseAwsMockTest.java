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
package org.apache.iceberg.aws.moto;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

@SuppressWarnings({"VisibilityModifier", "HideUtilityClassConstructor"})
public class BaseAwsMockTest {
  protected static final MotoContainer MOTO_CONTAINER = new MotoContainer();

  @BeforeAll
  public static void beforeAll() {
    MOTO_CONTAINER.start();

    // Set dummy AWS environment variables
    System.setProperty("aws.accessKeyId", "testing");
    System.setProperty("aws.secretAccessKey", "testing");
    System.setProperty("aws.sessionToken", "testing");
    System.setProperty("aws.region", "us-east-1");
  }

  @AfterAll
  public static void afterAll() {
    MOTO_CONTAINER.stop();

    // Unset dummy AWS environment variables
    System.clearProperty("aws.accessKeyId");
    System.clearProperty("aws.secretAccessKey");
    System.clearProperty("aws.sessionToken");
    System.clearProperty("aws.region");
  }

  public static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  public static String genRandomBucketName() {
    return String.format("bucket-%s", genRandomName());
  }
}
