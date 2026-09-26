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
package org.apache.iceberg.rest.events;

/** Standard catalog operation types carried by an event. */
public final class OperationType {
  public static final String CREATE_TABLE = "create-table";
  public static final String REGISTER_TABLE = "register-table";
  public static final String DROP_TABLE = "drop-table";
  public static final String UPDATE_TABLE = "update-table";
  public static final String RENAME_TABLE = "rename-table";
  public static final String CREATE_VIEW = "create-view";
  public static final String DROP_VIEW = "drop-view";
  public static final String UPDATE_VIEW = "update-view";
  public static final String RENAME_VIEW = "rename-view";
  public static final String CREATE_NAMESPACE = "create-namespace";
  public static final String UPDATE_NAMESPACE_PROPERTIES = "update-namespace-properties";
  public static final String DROP_NAMESPACE = "drop-namespace";

  private OperationType() {}
}
