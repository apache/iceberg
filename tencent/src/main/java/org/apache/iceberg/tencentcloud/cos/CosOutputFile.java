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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.metrics.MetricsContext;

class CosOutputFile extends BaseCosFile implements OutputFile {

  CosOutputFile(COS client, CosURI uri, CosProperties cosProperties, MetricsContext metrics) {
    super(client, uri, cosProperties, metrics);
  }

  static CosOutputFile fromLocation(COS client, String location, CosProperties cosProperties) {
    return new CosOutputFile(
        client, new CosURI(location), cosProperties, MetricsContext.nullMetrics());
  }

  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Location already exists: %s", uri());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return new CosOutputStream(client(), uri(), cosProperties(), metrics());
  }

  @Override
  public InputFile toInputFile() {
    return new CosInputFile(client(), uri(), cosProperties(), metrics());
  }
}
