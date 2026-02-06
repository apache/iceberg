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
import org.apache.iceberg.dell.DellProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;

class EcsOutputFile extends BaseEcsFile implements OutputFile {

  public static EcsOutputFile fromLocation(String location, S3Client client) {
    return new EcsOutputFile(
        client, new EcsURI(location), new DellProperties(), MetricsContext.nullMetrics());
  }

  public static EcsOutputFile fromLocation(
      String location, S3Client client, DellProperties dellProperties) {
    return new EcsOutputFile(
        client, new EcsURI(location), dellProperties, MetricsContext.nullMetrics());
  }

  static EcsOutputFile fromLocation(
      String location, S3Client client, DellProperties dellProperties, MetricsContext metrics) {
    return new EcsOutputFile(client, new EcsURI(location), dellProperties, metrics);
  }

  EcsOutputFile(
      S3Client client, EcsURI uri, DellProperties dellProperties, MetricsContext metrics) {
    super(client, uri, dellProperties, metrics);
  }

  /**
   * Create an output stream for the specified location if the target object does not exist in ECS
   * at the time of invocation.
   *
   * @return output stream
   */
  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("ECS object already exists: %s", uri());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return EcsAppendOutputStream.create(client(), uri(), metrics());
  }

  @Override
  public InputFile toInputFile() {
    return new EcsInputFile(client(), uri(), dellProperties(), metrics());
  }
}
