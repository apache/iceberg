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

package org.apache.iceberg;


import java.nio.ByteBuffer;

/**
 * Iceberg internally tracked field level metrics.
 */
public class FieldMetrics {
  private final int id;
  private final long valueCount;
  private final long nullValueCount;
  private final long nanValueCount;
  private final ByteBuffer lowerBound;
  private final ByteBuffer upperBound;

  public FieldMetrics(int id,
                      long valueCount,
                      long nullValueCount,
                      long nanValueCount,
                      ByteBuffer lowerBound,
                      ByteBuffer upperBound) {
    this.id = id;
    this.valueCount = valueCount;
    this.nullValueCount = nullValueCount;
    this.nanValueCount = nanValueCount;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  public int getId() {
    return id;
  }

  public long getValueCount() {
    return valueCount;
  }

  public long getNullValueCount() {
    return nullValueCount;
  }

  public long getNanValueCount() {
    return nanValueCount;
  }

  public ByteBuffer getLowerBound() {
    return lowerBound;
  }

  public ByteBuffer getUpperBound() {
    return upperBound;
  }
}
