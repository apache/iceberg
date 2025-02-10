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
package org.apache.iceberg.io.datafile;

import org.apache.iceberg.FileFormat;

/** Base implementation for the {@link ReaderService}es and {@link WriterService}es. */
public class ServiceBase implements DataFileServiceRegistry.TypedService {
  private final FileFormat format;
  private final String dataType;
  private final String serviceType;

  public ServiceBase(FileFormat format, String dataType, String serviceType) {
    this.format = format;
    this.dataType = dataType;
    this.serviceType = serviceType;
  }

  public ServiceBase(FileFormat format, String dataType) {
    this.format = format;
    this.dataType = dataType;
    this.serviceType = null;
  }

  @Override
  public FileFormat format() {
    return format;
  }

  @Override
  public String dataType() {
    return dataType;
  }

  @Override
  public String serviceType() {
    return serviceType;
  }
}
