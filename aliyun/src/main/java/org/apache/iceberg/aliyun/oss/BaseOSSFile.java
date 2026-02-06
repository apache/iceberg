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
package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.SimplifiedObjectMeta;
import org.apache.iceberg.aliyun.AliyunProperties;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

abstract class BaseOSSFile {
  private final OSS client;
  private final OSSURI uri;
  private final AliyunProperties aliyunProperties;
  private SimplifiedObjectMeta metadata;
  private final MetricsContext metrics;

  BaseOSSFile(OSS client, OSSURI uri, AliyunProperties aliyunProperties, MetricsContext metrics) {
    this.client = client;
    this.uri = uri;
    this.aliyunProperties = aliyunProperties;
    this.metrics = metrics;
  }

  public String location() {
    return uri.location();
  }

  public OSS client() {
    return client;
  }

  public OSSURI uri() {
    return uri;
  }

  public AliyunProperties aliyunProperties() {
    return aliyunProperties;
  }

  public boolean exists() {
    try {
      return objectMetadata() != null;
    } catch (OSSException e) {

      if (e.getErrorCode().equals(OSSErrorCode.NO_SUCH_BUCKET)
          || e.getErrorCode().equals(OSSErrorCode.NO_SUCH_KEY)) {
        return false;
      }

      throw e;
    }
  }

  protected SimplifiedObjectMeta objectMetadata() {
    if (metadata == null) {
      metadata = client.getSimplifiedObjectMeta(uri().bucket(), uri().key());
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
