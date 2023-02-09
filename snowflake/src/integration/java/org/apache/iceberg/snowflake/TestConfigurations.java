/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.snowflake;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.util.EnvironmentUtil;

class TestConfigurations {
  static final TestConfigurations instance = new TestConfigurations();
  private Map<String, String> properties;

  private TestConfigurations() {
    properties = new HashMap<String, String>();
    properties.put(CatalogProperties.URI, "env:" + "SNOW_URI");
    properties.put(CatalogProperties.USER, "env:" + "SNOW_USER");
    properties.put("password", "env:" + "SNOW_PASSWORD");
    properties.put("warehouse", "env:" + "SNOW_WAREHOUSE");
    properties.put("database", "env:" + "SNOW_TEST_DB_NAME");
    properties.put("externalVolume", "env:" + "SNOW_EXTERNAL_VOL_NAME");

    properties = EnvironmentUtil.resolveAll(properties);
  }

  String getURI() {
    return properties.get(CatalogProperties.URI);
  }

  String getUser() {
    return properties.get(CatalogProperties.USER);
  }

  String getWarehouse() {
    return properties.get("warehouse");
  }

  String getDatabase() {
    return properties.get("database");
  }

  String getExternalVolume() {
    return properties.get("externalVolume");
  }

  Map<String, String> getProperties() {
    return properties;
  }

  Map<String, String> getJdbcProperties() {
    Map<String,String> jdbcProps = new HashMap<String, String>();
    properties.forEach( (k,v) -> jdbcProps.put("jdbc."+k, v));
    return jdbcProps;
  }

  static TestConfigurations getInstance() {
    return instance;
  }
}
