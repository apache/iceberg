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
import com.obs.services.exception.ObsException;
import com.obs.services.model.ObjectMetadata;
import org.apache.http.HttpStatus;
import org.apache.iceberg.huaweicloud.HuaweicloudProperties;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseOBSFile {
  private static final Logger LOG = LoggerFactory.getLogger(BaseOBSFile.class);
  private final IObsClient client;
  private final OBSURI uri;
  private HuaweicloudProperties huaweicloudProperties;
  private ObjectMetadata metadata;
  private final MetricsContext metrics;

  BaseOBSFile(
      IObsClient client,
      OBSURI uri,
      HuaweicloudProperties huaweicloudProperties,
      MetricsContext metrics) {
    this.client = client;
    this.uri = uri;
    this.huaweicloudProperties = huaweicloudProperties;
    this.metrics = metrics;
  }

  public String location() {
    return uri.location();
  }

  public IObsClient client() {
    return client;
  }

  public OBSURI uri() {
    return uri;
  }

  public HuaweicloudProperties huaweicloudProperties() {
    return huaweicloudProperties;
  }

  public boolean exists() {
    try {
      return objectMetadata() != null;
    } catch (ObsException obsException) {

      if (obsException.getResponseCode() == HttpStatus.SC_NOT_FOUND) {
        return false;
      }

      throw obsException;
    }
  }

  protected ObjectMetadata objectMetadata() {
    if (metadata == null) {
      metadata = client.getObjectMetadata(uri().bucket(), uri().key());
    }
    LOG.debug("ObjectMetadata: {}", metadata);
    return metadata;
  }

  protected MetricsContext metrics() {
    return metrics;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("file", uri).toString();
  }
}
