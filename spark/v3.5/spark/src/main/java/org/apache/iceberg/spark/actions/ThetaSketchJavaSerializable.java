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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;

class ThetaSketchJavaSerializable implements Serializable {

  private Sketch sketch;

  ThetaSketchJavaSerializable() {}

  ThetaSketchJavaSerializable(final Sketch sketch) {
    this.sketch = sketch;
  }

  Sketch getSketch() {
    return sketch;
  }

  CompactSketch getCompactSketch() {
    if (sketch == null) {
      return null;
    }

    if (sketch instanceof UpdateSketch) {
      return sketch.compact();
    }

    return (CompactSketch) sketch;
  }

  void update(final ByteBuffer value) {
    if (sketch == null) {
      sketch = UpdateSketch.builder().build();
    }
    if (sketch instanceof UpdateSketch) {
      ((UpdateSketch) sketch).update(value);
    } else {
      throw new RuntimeException("update() on read-only sketch");
    }
  }

  double getEstimate() {
    if (sketch == null) {
      return 0.0;
    }
    return sketch.getEstimate();
  }

  private void writeObject(final ObjectOutputStream out) throws IOException {
    if (sketch == null) {
      out.writeInt(0);
      return;
    }
    final byte[] serializedSketchBytes = sketch.compact().toByteArray();
    out.writeInt(serializedSketchBytes.length);
    out.write(serializedSketchBytes);
  }

  private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
    final int length = in.readInt();
    if (length == 0) {
      return;
    }
    final byte[] serializedSketchBytes = new byte[length];
    in.readFully(serializedSketchBytes);
    sketch = CompactSketch.wrap(Memory.wrap(serializedSketchBytes));
  }
}
