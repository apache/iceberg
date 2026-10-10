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
package org.apache.iceberg.io;

import org.apache.iceberg.metrics.MetricsContext;

/** Base {@link FileIO} implementation with shared handling for HTTP(S) input locations. */
public abstract class BaseFileIO implements FileIO {

  @Override
  public final InputFile newInputFile(String path) {
    if (HttpInputFile.isHttpUrl(path)) {
      return HttpInputFile.fromLocation(path, properties(), metrics());
    }

    return newInputFileForLocation(path);
  }

  @Override
  public final InputFile newInputFile(String path, long length) {
    if (HttpInputFile.isHttpUrl(path)) {
      return HttpInputFile.fromLocation(path, length, properties(), metrics());
    }

    return newInputFileForLocation(path, length);
  }

  protected MetricsContext metrics() {
    return MetricsContext.nullMetrics();
  }

  protected abstract InputFile newInputFileForLocation(String path);

  protected abstract InputFile newInputFileForLocation(String path, long length);
}
