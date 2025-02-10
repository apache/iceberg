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

import org.apache.iceberg.io.OutputFile;

/**
 * Base implementation for the {@link AppenderBuilder} which handles the common attributes for the
 * builders. FileFormat implementations should extend this for creating their own appender builders.
 *
 * @param <T> the type of the builder for chaining
 */
public abstract class AppenderBuilderBase<T extends AppenderBuilderBase<T>>
    extends WriterBuilderBase<T> implements AppenderBuilder<T> {
  private String name = "table";

  protected AppenderBuilderBase(OutputFile file) {
    super(file);
  }

  @Override
  public T named(String newName) {
    this.name = newName;
    return (T) this;
  }

  protected String name() {
    return name;
  }
}
