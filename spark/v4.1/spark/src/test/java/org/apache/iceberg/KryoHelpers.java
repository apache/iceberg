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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

public class KryoHelpers {

  private KryoHelpers() {}

  @SuppressWarnings("unchecked")
  public static <T> T roundTripSerialize(T obj) throws IOException {
    Kryo kryo = new KryoSerializer(new SparkConf()).newKryo();

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    try (Output out = new Output(new ObjectOutputStream(bytes))) {
      kryo.writeClassAndObject(out, obj);
    }

    try (Input in =
        new Input(new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray())))) {
      return (T) kryo.readClassAndObject(in);
    }
  }
}
