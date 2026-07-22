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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Projector} to cover behaviour that can't be tested in {@code
 * TestFlinkTableSource}.
 */
public class TestProjector {

  // id INT, person ROW<name STRING, age INT>, address ROW<city STRING, geo ROW<lat INT, lng INT>>
  private static final RowType PERSON =
      row(new String[] {"name", "age"}, VarCharType.STRING_TYPE, new IntType());
  private static final RowType GEO = row(new String[] {"lat", "lng"}, new IntType(), new IntType());
  private static final RowType ADDRESS =
      row(new String[] {"city", "geo"}, VarCharType.STRING_TYPE, GEO);
  private static final RowType ORIGINAL =
      row(new String[] {"id", "person", "address"}, new IntType(), PERSON, ADDRESS);

  private static RowType row(String[] names, LogicalType... types) {
    return RowType.of(types, names);
  }

  private static Projector projector(int[][] projectedFields) {
    RowType producedRowType = (RowType) Projection.of(projectedFields).project(ORIGINAL);
    return Projector.of(ORIGINAL, projectedFields, producedRowType);
  }

  /** A source stream shaped like the reader output for the given projection (never executed). */
  private static DataStream<RowData> readerStream(int[][] projectedFields) {
    RowType readSchema = projector(projectedFields).readSchema();
    return StreamExecutionEnvironment.getExecutionEnvironment()
        .fromData(InternalTypeInfo.of(readSchema), new GenericRowData(readSchema.getFieldCount()));
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTripSerialize(T instance) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(instance);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  @Test
  public void addsNoMapForInOrderTopLevelProjection() {
    // A non-nested, in-order projection already matches the produced shape, so the reader stream is
    // returned unchanged (no map operator).
    for (int[][] inOrder :
        new int[][][] {
          {{0}}, // select only first column
          {{1}}, // select only second column
          {{2}}, // select only third column
          {{0}, {1}}, // select first two columns (in order)
          {{1}, {2}}, // select last two columns (in order)
          {{0}, {1}, {2}} // select all three columns (in order)
        }) {
      DataStream<RowData> source = readerStream(inOrder);
      assertThat(projector(inOrder).project(source)).isSameAs(source);
    }
  }

  @Test
  public void buildsFieldGetterForUnknownTypeLeaf() {
    // Iceberg `unknown` columns surface as Flink NullType. Building the projector builds a field
    // getter for the NullType leaf, which must not fail on the NullType root. This is why
    // Projector uses FlinkRowData.createFieldGetter (see FLINK-37245) instead of Flink's built-in
    // RowData.createFieldGetter, which throws. A NullType column cannot be run through a real
    // DataStream (Flink has no serializer for it), so this guards getter construction only.
    RowType original = row(new String[] {"meta"}, row(new String[] {"note"}, new NullType()));
    int[][] projection = {{0, 0}};
    RowType produced = (RowType) Projection.of(projection).project(original);

    Projector projector = Projector.of(original, projection, produced);

    assertThat(projector.readSchema()).isEqualTo(original);
  }

  @Test
  public void rejectsDescentIntoNonStruct() {
    // Flink never pushes a path that descends into a scalar, so this guardrail is not reachable via
    // SQL; assert it directly.
    assertThatThrownBy(() -> Projector.of(ORIGINAL, new int[][] {{0, 0}}, ORIGINAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot project subfields of non-struct field <id>");
  }

  @Test
  public void isSerializable() throws Exception {
    // Flink ships the projection's map function to task managers, so it must survive Java
    // serialization (in particular the nested field getters). A successful round-trip proves the
    // whole projector -- including the shipped map -- serializes.
    Projector projector = projector(new int[][] {{1, 0}, {0}});

    Projector roundTripped = roundTripSerialize(projector);

    assertThat(roundTripped.readSchema()).isEqualTo(projector.readSchema());
  }
}
