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
package org.apache.iceberg.mr.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.Writable;

/**
 * A simple container of objects that you can get and set.
 *
 * @param <T> the Java type of the object held by this container
 */
public class Container<T> implements Writable {

  private T value;

  public T get() {
    return value;
  }

  public void set(T newValue) {
    this.value = newValue;
  }

  @Override
  public void write(DataOutput dataOutput) {
    throw new UnsupportedOperationException("write is not supported.");
  }

  @Override
  public void readFields(DataInput dataInput) {
    throw new UnsupportedOperationException("readFields is not supported.");
  }
}
