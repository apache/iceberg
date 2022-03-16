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

package org.apache.iceberg.azure.blob;

public class AzureBlobTestConstants {

  public static final String STORAGE_ACCOUNT_1 = System.getenv("STORAGE_ACCOUNT_1");
  public static final String STORAGE_ACCOUNT_1_KEY = System.getenv("STORAGE_ACCOUNT_1_KEY");
  public static final String STORAGE_ACCOUNT_2 = System.getenv("STORAGE_ACCOUNT_2");
  public static final String STORAGE_ACCOUNT_2_KEY = System.getenv("STORAGE_ACCOUNT_2_KEY");
  public static final String STORAGE_ACCOUNT_3 = System.getenv("STORAGE_ACCOUNT_3");
  public static final String STORAGE_ACCOUNT_3_KEY = System.getenv("STORAGE_ACCOUNT_3_KEY");
  public static final String DATA_CONTAINER = "data";

  private AzureBlobTestConstants() {
  }
}
