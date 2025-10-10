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
package org.apache.iceberg.geospatial;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

public class TestBoundingBox {

  @Test
  public void testConstructorAndAccessors() {
    GeospatialBound min = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max = GeospatialBound.createXY(3.0, 4.0);

    BoundingBox box = new BoundingBox(min, max);

    assertThat(box.min()).isEqualTo(min);
    assertThat(box.max()).isEqualTo(max);
    assertThat(box.min().x()).isEqualTo(1.0);
    assertThat(box.min().y()).isEqualTo(2.0);
    assertThat(box.max().x()).isEqualTo(3.0);
    assertThat(box.max().y()).isEqualTo(4.0);
  }

  @Test
  public void testCreateFromByteBuffers() {
    // Create byte buffers for XY bounds
    ByteBuffer minBuffer = ByteBuffer.allocate(16);
    minBuffer.order(ByteOrder.LITTLE_ENDIAN);
    minBuffer.putDouble(0, 1.0); // x
    minBuffer.putDouble(8, 2.0); // y

    ByteBuffer maxBuffer = ByteBuffer.allocate(16);
    maxBuffer.order(ByteOrder.LITTLE_ENDIAN);
    maxBuffer.putDouble(0, 3.0); // x
    maxBuffer.putDouble(8, 4.0); // y

    BoundingBox box = BoundingBox.fromByteBuffers(minBuffer, maxBuffer);

    assertThat(box.min().x()).isEqualTo(1.0);
    assertThat(box.min().y()).isEqualTo(2.0);
    assertThat(box.max().x()).isEqualTo(3.0);
    assertThat(box.max().y()).isEqualTo(4.0);
    assertThat(minBuffer.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
    assertThat(maxBuffer.order()).isEqualTo(ByteOrder.LITTLE_ENDIAN);
  }

  @Test
  public void testCreateFromBigEndianByteBuffers() {
    // Create byte buffers for XY bounds
    ByteBuffer minBuffer = ByteBuffer.allocate(16);
    minBuffer.order(ByteOrder.LITTLE_ENDIAN);
    minBuffer.putDouble(0, 10.0); // x
    minBuffer.putDouble(8, 20.0); // y
    minBuffer.order(ByteOrder.BIG_ENDIAN);

    ByteBuffer maxBuffer = ByteBuffer.allocate(16);
    maxBuffer.order(ByteOrder.LITTLE_ENDIAN);
    maxBuffer.putDouble(0, 30.0); // x
    maxBuffer.putDouble(8, 40.0); // y
    maxBuffer.order(ByteOrder.BIG_ENDIAN);

    BoundingBox box = BoundingBox.fromByteBuffers(minBuffer, maxBuffer);

    assertThat(box.min().x()).isEqualTo(10.0);
    assertThat(box.min().y()).isEqualTo(20.0);
    assertThat(box.max().x()).isEqualTo(30.0);
    assertThat(box.max().y()).isEqualTo(40.0);
    assertThat(minBuffer.order()).isEqualTo(ByteOrder.BIG_ENDIAN);
    assertThat(maxBuffer.order()).isEqualTo(ByteOrder.BIG_ENDIAN);
  }

  @Test
  public void testEqualsAndHashCode() {
    GeospatialBound min1 = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max1 = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox box1 = new BoundingBox(min1, max1);

    // Same values
    GeospatialBound min2 = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max2 = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox box2 = new BoundingBox(min2, max2);

    // Different values
    GeospatialBound min3 = GeospatialBound.createXY(0.0, 0.0);
    GeospatialBound max3 = GeospatialBound.createXY(10.0, 10.0);
    BoundingBox box3 = new BoundingBox(min3, max3);

    // Test equals
    assertThat(box1).isEqualTo(box2);
    assertThat(box1).isNotEqualTo(box3);
    assertThat(box1).isNotEqualTo(null);
    assertThat(box1).isNotEqualTo("not a box");

    // Test hashCode
    assertThat(box1.hashCode()).isEqualTo(box2.hashCode());
    assertThat(box1.hashCode()).isNotEqualTo(box3.hashCode());
  }

  @Test
  public void testToString() {
    GeospatialBound min = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox box = new BoundingBox(min, max);
    assertThat(box.toString()).isEqualTo("BoundingBox{min={x=1.0, y=2.0}, max={x=3.0, y=4.0}}");
  }

  @Test
  public void testRoundTripSerDe() {
    GeospatialBound min = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound max = GeospatialBound.createXY(3.0, 4.0);
    BoundingBox original = new BoundingBox(min, max);
    BoundingBox deserialized = roundTripSerDe(original);
    assertThat(deserialized).isEqualTo(original);
  }

  private BoundingBox roundTripSerDe(BoundingBox original) {
    ByteBuffer buffer = original.toByteBuffer();
    return BoundingBox.fromByteBuffer(buffer);
  }
}
