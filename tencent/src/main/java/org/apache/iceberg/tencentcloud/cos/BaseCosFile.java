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
package org.apache.iceberg.tencentcloud.cos;

import com.qcloud.cos.COS;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.ObjectMetadata;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

abstract class BaseCosFile {
  private final COS client;
  private final CosURI uri;
  private final CosProperties cosProperties;
  private final MetricsContext metrics;
  private ObjectMetadata metadata;

  BaseCosFile(COS client, CosURI uri, CosProperties cosProperties, MetricsContext metrics) {
    this.client = client;
    this.uri = uri;
    this.cosProperties = cosProperties;
    this.metrics = metrics;
  }

  public String location() {
    return uri.location();
  }

  public COS client() {
    return client;
  }

  public CosURI uri() {
    return uri;
  }

  public CosProperties cosProperties() {
    return cosProperties;
  }

  public boolean exists() {
    try {
      return objectMetadata() != null;
    } catch (CosServiceException e) {
      if (e.getStatusCode() == 404) {
        return false;
      } else {
        throw e;
      }
    }
  }

  protected ObjectMetadata objectMetadata() {
    if (metadata == null) {
      metadata = client.getObjectMetadata(uri().bucket(), uri().key());
    }
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
