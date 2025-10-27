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
import org.apache.iceberg.util.ByteBuffers;
import org.junit.jupiter.api.Test;

public class TestGeospatialBound {

  @Test
  public void testCreateXY() {
    GeospatialBound bound = GeospatialBound.createXY(1.0, 2.0);
    assertThat(bound.x()).isEqualTo(1.0);
    assertThat(bound.y()).isEqualTo(2.0);
    assertThat(bound.hasZ()).isFalse();
    assertThat(bound.hasM()).isFalse();
    assertThat(Double.isNaN(bound.z())).isTrue();
    assertThat(Double.isNaN(bound.m())).isTrue();
  }

  @Test
  public void testCreateXYZ() {
    GeospatialBound bound = GeospatialBound.createXYZ(1.0, 2.0, 3.0);
    assertThat(bound.x()).isEqualTo(1.0);
    assertThat(bound.y()).isEqualTo(2.0);
    assertThat(bound.z()).isEqualTo(3.0);
    assertThat(bound.hasZ()).isTrue();
    assertThat(bound.hasM()).isFalse();
    assertThat(Double.isNaN(bound.m())).isTrue();
  }

  @Test
  public void testCreateXYM() {
    GeospatialBound bound = GeospatialBound.createXYM(1.0, 2.0, 4.0);
    assertThat(bound.x()).isEqualTo(1.0);
    assertThat(bound.y()).isEqualTo(2.0);
    assertThat(bound.m()).isEqualTo(4.0);
    assertThat(bound.hasZ()).isFalse();
    assertThat(bound.hasM()).isTrue();
    assertThat(Double.isNaN(bound.z())).isTrue();
  }

  @Test
  public void testCreateXYZM() {
    GeospatialBound bound = GeospatialBound.createXYZM(1.0, 2.0, 3.0, 4.0);
    assertThat(bound.x()).isEqualTo(1.0);
    assertThat(bound.y()).isEqualTo(2.0);
    assertThat(bound.z()).isEqualTo(3.0);
    assertThat(bound.m()).isEqualTo(4.0);
    assertThat(bound.hasZ()).isTrue();
    assertThat(bound.hasM()).isTrue();
  }

  @Test
  public void testEqualsAndHashCode() {
    GeospatialBound xy1 = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound xy2 = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound xy3 = GeospatialBound.createXY(2.0, 1.0);
    assertThat(xy1).isEqualTo(xy2);
    assertThat(xy1.hashCode()).isEqualTo(xy2.hashCode());
    assertThat(xy1).isNotEqualTo(xy3);

    GeospatialBound xyz1 = GeospatialBound.createXYZ(1.0, 2.0, 3.0);
    GeospatialBound xyz2 = GeospatialBound.createXYZ(1.0, 2.0, 3.0);
    GeospatialBound xyz3 = GeospatialBound.createXYZ(1.0, 2.0, 4.0);
    assertThat(xyz1).isEqualTo(xyz2);
    assertThat(xyz1.hashCode()).isEqualTo(xyz2.hashCode());
    assertThat(xyz1).isNotEqualTo(xyz3);
    assertThat(xyz1).isNotEqualTo(xy1);

    GeospatialBound xym1 = GeospatialBound.createXYM(1.0, 2.0, 4.0);
    GeospatialBound xym2 = GeospatialBound.createXYM(1.0, 2.0, 4.0);
    GeospatialBound xym3 = GeospatialBound.createXYM(1.0, 2.0, 5.0);
    assertThat(xym1).isEqualTo(xym2);
    assertThat(xym1.hashCode()).isEqualTo(xym2.hashCode());
    assertThat(xym1).isNotEqualTo(xym3);
    assertThat(xym1).isNotEqualTo(xy1);

    GeospatialBound xyzm1 = GeospatialBound.createXYZM(1.0, 2.0, 3.0, 4.0);
    GeospatialBound xyzm2 = GeospatialBound.createXYZM(1.0, 2.0, 3.0, 4.0);
    GeospatialBound xyzm3 = GeospatialBound.createXYZM(1.0, 2.0, 3.0, 5.0);
    assertThat(xyzm1).isEqualTo(xyzm2);
    assertThat(xyzm1.hashCode()).isEqualTo(xyzm2.hashCode());
    assertThat(xyzm1).isNotEqualTo(xyzm3);
    assertThat(xyzm1).isNotEqualTo(xyz1);
  }

  @Test
  public void testToString() {
    GeospatialBound xy = GeospatialBound.createXY(1.0, 2.0);
    assertThat(xy.toString()).isEqualTo("GeospatialBound(x=1.0, y=2.0)");

    GeospatialBound xyz = GeospatialBound.createXYZ(1.0, 2.0, 3.0);
    assertThat(xyz.toString()).isEqualTo("GeospatialBound(x=1.0, y=2.0, z=3.0)");

    GeospatialBound xym = GeospatialBound.createXYM(1.0, 2.0, 4.0);
    assertThat(xym.toString()).isEqualTo("GeospatialBound(x=1.0, y=2.0, m=4.0)");

    GeospatialBound xyzm = GeospatialBound.createXYZM(1.0, 2.0, 3.0, 4.0);
    assertThat(xyzm.toString()).isEqualTo("GeospatialBound(x=1.0, y=2.0, z=3.0, m=4.0)");
  }

  @Test
  public void testSimpleString() {
    GeospatialBound xy = GeospatialBound.createXY(1.0, 2.0);
    assertThat(xy.simpleString()).isEqualTo("x=1.0, y=2.0");

    GeospatialBound xyz = GeospatialBound.createXYZ(1.0, 2.0, 3.0);
    assertThat(xyz.simpleString()).isEqualTo("x=1.0, y=2.0, z=3.0");

    GeospatialBound xym = GeospatialBound.createXYM(1.0, 2.0, 4.0);
    assertThat(xym.simpleString()).isEqualTo("x=1.0, y=2.0, m=4.0");

    GeospatialBound xyzm = GeospatialBound.createXYZM(1.0, 2.0, 3.0, 4.0);
    assertThat(xyzm.simpleString()).isEqualTo("x=1.0, y=2.0, z=3.0, m=4.0");
  }

  @Test
  public void testSerde() {
    // Test XY format (16 bytes: x:y)
    // These bytes represent x=10.0, y=13.0
    byte[] xyBytes =
        new byte[] {
          0, 0, 0, 0, 0, 0, 36, 64, // 10.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 42, 64 // 13.0 in little-endian IEEE 754
        };
    GeospatialBound xy = GeospatialBound.fromByteArray(xyBytes);
    assertThat(xy.x()).isEqualTo(10.0);
    assertThat(xy.y()).isEqualTo(13.0);
    assertThat(xy.hasZ()).isFalse();
    assertThat(xy.hasM()).isFalse();
    assertThat(ByteBuffers.toByteArray(xy.toByteBuffer())).isEqualTo(xyBytes);

    // Test XYZ format (24 bytes: x:y:z)
    // These bytes represent x=10.0, y=13.0, z=15.0
    byte[] xyzBytes =
        new byte[] {
          0, 0, 0, 0, 0, 0, 36, 64, // 10.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 42, 64, // 13.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 46, 64 // 15.0 in little-endian IEEE 754
        };
    GeospatialBound xyz = GeospatialBound.fromByteArray(xyzBytes);
    assertThat(xyz.x()).isEqualTo(10.0);
    assertThat(xyz.y()).isEqualTo(13.0);
    assertThat(xyz.z()).isEqualTo(15.0);
    assertThat(xyz.hasZ()).isTrue();
    assertThat(xyz.hasM()).isFalse();
    assertThat(ByteBuffers.toByteArray(xyz.toByteBuffer())).isEqualTo(xyzBytes);

    // Test XYM format (32 bytes: x:y:NaN:m)
    // These bytes represent x=10.0, y=13.0, z=NaN, m=20.0
    byte[] xymBytes =
        new byte[] {
          0, 0, 0, 0, 0, 0, 36, 64, // 10.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 42, 64, // 13.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, (byte) 248, 127, // NaN in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 52, 64 // 20.0 in little-endian IEEE 754
        };
    GeospatialBound xym = GeospatialBound.fromByteArray(xymBytes);
    assertThat(xym.x()).isEqualTo(10.0);
    assertThat(xym.y()).isEqualTo(13.0);
    assertThat(Double.isNaN(xym.z())).isTrue();
    assertThat(xym.m()).isEqualTo(20.0);
    assertThat(xym.hasZ()).isFalse();
    assertThat(xym.hasM()).isTrue();
    assertThat(ByteBuffers.toByteArray(xym.toByteBuffer())).isEqualTo(xymBytes);

    // Test XYZM format (32 bytes: x:y:z:m)
    // These bytes represent x=10.0, y=13.0, z=15.0, m=20.0
    byte[] xyzmBytes =
        new byte[] {
          0, 0, 0, 0, 0, 0, 36, 64, // 10.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 42, 64, // 13.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 46, 64, // 15.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 52, 64 // 20.0 in little-endian IEEE 754
        };
    GeospatialBound xyzm = GeospatialBound.fromByteArray(xyzmBytes);
    assertThat(xyzm.x()).isEqualTo(10.0);
    assertThat(xyzm.y()).isEqualTo(13.0);
    assertThat(xyzm.z()).isEqualTo(15.0);
    assertThat(xyzm.m()).isEqualTo(20.0);
    assertThat(xyzm.hasZ()).isTrue();
    assertThat(xyzm.hasM()).isTrue();
    assertThat(ByteBuffers.toByteArray(xyzm.toByteBuffer())).isEqualTo(xyzmBytes);
  }

  @Test
  public void testFromByteBuffer() {
    // Test XY format (16 bytes: x:y)
    // These bytes represent x=10.0, y=13.0
    byte[] xyBytes =
        new byte[] {
          0, 0, 0, 0, 0, 0, 36, 64, // 10.0 in little-endian IEEE 754
          0, 0, 0, 0, 0, 0, 42, 64 // 13.0 in little-endian IEEE 754
        };
    ByteBuffer xyBuffer = ByteBuffer.wrap(xyBytes);
    for (ByteOrder endianness : new ByteOrder[] {ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN}) {
      xyBuffer.order(endianness);
      GeospatialBound xy = GeospatialBound.fromByteBuffer(xyBuffer);
      assertThat(xy.x()).isEqualTo(10.0);
      assertThat(xy.y()).isEqualTo(13.0);
      assertThat(xy.hasZ()).isFalse();
      assertThat(xy.hasM()).isFalse();
      assertThat(xyBuffer.position()).isEqualTo(0);
      assertThat(xyBuffer.order()).isEqualTo(endianness);
    }
  }

  private GeospatialBound roundTripSerDe(GeospatialBound original) {
    ByteBuffer buffer = original.toByteBuffer();
    return GeospatialBound.fromByteBuffer(buffer);
  }

  @Test
  public void testRoundTripSerDe() {
    // Test XY serialization
    GeospatialBound xy = GeospatialBound.createXY(1.1, 2.2);
    assertThat(roundTripSerDe(xy)).isEqualTo(xy);

    // Test XYZ serialization
    GeospatialBound xyz = GeospatialBound.createXYZ(1.1, 2.2, 3.3);
    assertThat(roundTripSerDe(xyz)).isEqualTo(xyz);

    // Test XYM serialization
    GeospatialBound xym = GeospatialBound.createXYM(1.1, 2.2, 4.4);
    assertThat(roundTripSerDe(xym)).isEqualTo(xym);

    // Test XYZM serialization
    GeospatialBound xyzm = GeospatialBound.createXYZM(1.1, 2.2, 3.3, 4.4);
    assertThat(roundTripSerDe(xyzm)).isEqualTo(xyzm);
  }
}
