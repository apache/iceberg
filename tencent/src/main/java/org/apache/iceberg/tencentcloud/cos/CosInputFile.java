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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;

class CosInputFile extends BaseCosFile implements InputFile {

  private Long length = null;

  CosInputFile(COS client, CosURI uri, CosProperties cosProperties, MetricsContext metrics) {
    super(client, uri, cosProperties, metrics);
  }

  CosInputFile(
      COS client, CosURI uri, CosProperties cosProperties, long length, MetricsContext metrics) {
    super(client, uri, cosProperties, metrics);
    ValidationException.check(length >= 0, "Invalid file length: %s", length);
    this.length = length;
  }

  @Override
  public long getLength() {
    if (length == null) {
      length = objectMetadata().getContentLength();
    }
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new CosInputStream(client(), uri(), metrics());
  }
}
