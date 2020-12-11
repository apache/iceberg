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

package org.apache.iceberg.util.conf;

import java.util.Map;

public class Properties extends Conf {
  private final Map<String, String> properties;

  protected Properties(Map<String, String> properties) {
    this.properties = properties;
  }

  protected Properties(String namespace, Map<String, String> properties) {
    super(namespace);
    this.properties = properties;
  }

  public static Properties of(String namespace, Map<String, String> properties) {
    return new Properties(namespace, properties);
  }

  public static Properties of(Map<String, String> properties) {
    return new Properties(properties);
  }

  @Override
  protected String get(String key) {
    return properties.get(key);
  }

  @Override
  protected void set(String key, String value) {
    properties.put(key, value);
  }
}
