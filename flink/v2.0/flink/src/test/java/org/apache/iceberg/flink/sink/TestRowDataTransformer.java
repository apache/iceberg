/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink;

import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestRowDataTransformer {

    @Test
    public void testPrimitiveTypes() {
        testConverter(new DataGenerators.Primitives());
    }

    @Test
    public void testStructOfPrimitive() {
        testConverter(new DataGenerators.StructOfPrimitive());
    }

    @Test
    public void testStructOfArray() {
        testConverter(new DataGenerators.StructOfArray());
    }

    @Test
    public void testStructOfMap() {
        testConverter(new DataGenerators.StructOfMap());
    }

    @Test
    public void testStructOfStruct() {
        testConverter(new DataGenerators.StructOfStruct());
    }

    @Test
    public void testArrayOfPrimitive() {
        testConverter(new DataGenerators.ArrayOfPrimitive());
    }

    @Test
    public void testArrayOfArray() {
        testConverter(new DataGenerators.ArrayOfArray());
    }

    @Test
    public void testArrayOfMap() {
        testConverter(new DataGenerators.ArrayOfMap());
    }

    @Test
    public void testArrayOfStruct() {
        testConverter(new DataGenerators.ArrayOfStruct());
    }

    @Test
    public void testMapOfPrimitives() {
        testConverter(new DataGenerators.MapOfPrimitives());
    }

    @Test
    public void testMapOfArray() {
        testConverter(new DataGenerators.MapOfArray());
    }

    @Test
    public void testMapOfMap() {
        testConverter(new DataGenerators.MapOfMap());
    }

    @Test
    public void testMapOfStruct() {
        testConverter(new DataGenerators.MapOfStruct());
    }

    protected void testConverter(DataGenerator dataGenerator) {
        RowDataTransformer transformer = new RowDataTransformer(dataGenerator.flinkRowType(), dataGenerator.icebergSchema().asStruct());
        RowData expected = dataGenerator.generateFlinkRowData();
        RowData actual = transformer.wrap(expected);

        // Run the serializer copy to ensure that the wrapped row data is at least accessible
        new RowDataSerializer(dataGenerator.flinkRowType()).copy(actual);
    }
    
    

    //    @Test
//    void testNoChange() {
//        Schema iSchema =
//                new Schema(
//                        Types.NestedField.required(4, "array", Types.ListType.ofOptional(5, Types.IntegerType.get())),
//                        Types.NestedField.required(1, "tinyint", Types.IntegerType.get()),
//                        Types.NestedField.required(2, "smallint", Types.IntegerType.get()),
//                        Types.NestedField.optional(3, "int", Types.IntegerType.get()));
//        TableSchema flinkSchema =
//                TableSchema.builder()
//                        .field("array", DataTypes.ARRAY(DataTypes.TINYINT()).notNull())
//                        .field("tinyint", DataTypes.TINYINT().notNull())
//                        .field("smallint", DataTypes.SMALLINT().notNull())
//                        .field("int", DataTypes.INT().nullable())
//                        .build();
//
//        List<RowData> rows =
//                Lists.newArrayList(
//                        GenericRowData.of(new GenericArrayData(new byte[] {(byte) 0x04, (byte) 0x05}), (byte) 0x01, (short) -32768, 101),
//                        GenericRowData.of(new GenericArrayData(new byte[] {(byte) 0x06, (byte) 0x07}), (byte) 0x02, (short) 0, 102),
//                        GenericRowData.of(new GenericArrayData(new byte[] {(byte) 0x08, (byte) 0x09}), (byte) 0x03, (short) 32767, 103));
//
//        RowDataTransformer transformer = new RowDataTransformer((RowType) flinkSchema.toRowDataType().getLogicalType(), iSchema.asStruct());
//
//        for (RowData row : rows) {
//            RowData transformed = transformer.wrap(row);
//            transformed.getInt(1);
//            assertThat(transformed.getArity()).isEqualTo(4);
//        }
//    }
}
