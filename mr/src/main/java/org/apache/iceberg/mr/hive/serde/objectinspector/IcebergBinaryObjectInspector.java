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

package org.apache.iceberg.mr.hive.serde.objectinspector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.iceberg.util.ByteBuffers;

public abstract class IcebergBinaryObjectInspector extends AbstractPrimitiveJavaObjectInspector
                                                   implements BinaryObjectInspector {

  private static final IcebergBinaryObjectInspector BYTE_ARRAY = new IcebergBinaryObjectInspector() {
    @Override
    byte[] toByteArray(Object o) {
      return (byte[]) o;
    }
  };

  private static final IcebergBinaryObjectInspector BYTE_BUFFER = new IcebergBinaryObjectInspector() {
    @Override
    byte[] toByteArray(Object o) {
      return ByteBuffers.toByteArray((ByteBuffer) o);
    }
  };

  public static IcebergBinaryObjectInspector byteArray() {
    return BYTE_ARRAY;
  }

  public static IcebergBinaryObjectInspector byteBuffer() {
    return BYTE_BUFFER;
  }

  private IcebergBinaryObjectInspector() {
    super(TypeInfoFactory.binaryTypeInfo);
  }

  abstract byte[] toByteArray(Object object);

  @Override
  public byte[] getPrimitiveJavaObject(Object o) {
    return toByteArray(o);
  }

  @Override
  public BytesWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new BytesWritable(getPrimitiveJavaObject(o));
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    byte[] bytes = (byte[]) o;
    return Arrays.copyOf(bytes, bytes.length);
  }

}
