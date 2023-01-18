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
package org.apache.iceberg.hudi;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseSnapshotHudiTableAction implements SnapshotHudiTable {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseSnapshotHudiTableAction.class.getName());

  private HoodieTableMetaClient HoodieMetaClient;

  public BaseSnapshotHudiTableAction(
      Configuration hoodieConfiguration, String hoodieTableBasePath) {
    this.HoodieMetaClient = buildTableMetaClient(hoodieConfiguration, hoodieTableBasePath);
  }

  @Override
  public SnapshotHudiTable tableProperties(Map<String, String> properties) {
    return null;
  }

  @Override
  public SnapshotHudiTable tableProperty(String key, String value) {
    return null;
  }

  @Override
  public Result execute() {
    LOG.info("Alpha test: hoodie table base path: {}", HoodieMetaClient.getBasePathV2());

    return null;
  }

  private static HoodieTableMetaClient buildTableMetaClient(Configuration conf, String basePath) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
    return metaClient;
  }
}
