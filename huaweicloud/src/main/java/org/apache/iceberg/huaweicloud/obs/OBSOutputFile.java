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
package org.apache.iceberg.huaweicloud.obs;

import com.obs.services.IObsClient;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.huaweicloud.HuaweicloudProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OBSOutputFile extends BaseOBSFile implements OutputFile {
  private static final Logger LOG = LoggerFactory.getLogger(BaseOBSFile.class);

  OBSOutputFile(
      IObsClient client,
      OBSURI uri,
      HuaweicloudProperties huaweicloudProperties,
      MetricsContext metrics) {
    super(client, uri, huaweicloudProperties, metrics);
  }

  static OBSOutputFile fromLocation(
      IObsClient client, String location, HuaweicloudProperties huaweicloudProperties) {
    return new OBSOutputFile(
        client, new OBSURI(location), huaweicloudProperties, MetricsContext.nullMetrics());
  }

  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      LOG.debug("Location exists: {}", uri());
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Location already exists: %s", uri());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return new OBSOutputStream(client(), uri(), huaweicloudProperties(), metrics());
  }

  @Override
  public InputFile toInputFile() {
    return new OBSInputFile(client(), uri(), huaweicloudProperties(), metrics());
  }
}
