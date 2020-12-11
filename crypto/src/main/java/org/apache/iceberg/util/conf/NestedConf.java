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

public class NestedConf extends Conf {
  private final String namespace;
  private final Conf conf;

  protected NestedConf(Conf conf, String namespace) {
    super(namespace);
    this.namespace = namespace;
    this.conf = conf;
  }

  public static NestedConf of(Conf conf, String suffix) {
    return new NestedConf(conf, suffix + ".");
  }

  @Override
  protected String get(String key) {
    return conf.propertyAsString(key, null);
  }

  @Override
  protected void set(String key, String value) {
    conf.setString(key, value);
  }
}
