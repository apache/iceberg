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
package org.apache.iceberg.dell.ecs;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import org.apache.iceberg.dell.DellProperties;
import org.apache.iceberg.metrics.MetricsContext;

abstract class BaseEcsFile {

  private final S3Client client;
  private final EcsURI uri;
  private final DellProperties dellProperties;
  private S3ObjectMetadata metadata;
  private final MetricsContext metrics;

  BaseEcsFile(S3Client client, EcsURI uri, DellProperties dellProperties, MetricsContext metrics) {
    this.client = client;
    this.uri = uri;
    this.dellProperties = dellProperties;
    this.metrics = metrics;
  }

  public String location() {
    return uri.location();
  }

  S3Client client() {
    return client;
  }

  EcsURI uri() {
    return uri;
  }

  public DellProperties dellProperties() {
    return dellProperties;
  }

  protected MetricsContext metrics() {
    return metrics;
  }

  /**
   * Note: this may be stale if file was deleted since metadata is cached for size/existence checks.
   *
   * @return flag
   */
  public boolean exists() {
    try {
      getObjectMetadata();
      return true;
    } catch (S3Exception e) {
      if (e.getHttpCode() == 404) {
        return false;
      } else {
        throw e;
      }
    }
  }

  protected S3ObjectMetadata getObjectMetadata() throws S3Exception {
    if (metadata == null) {
      metadata = client().getObjectMetadata(uri.bucket(), uri.name());
    }

    return metadata;
  }

  @Override
  public String toString() {
    return uri.toString();
  }
}
