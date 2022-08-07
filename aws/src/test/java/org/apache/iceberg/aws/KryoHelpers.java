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
package org.apache.iceberg.aws;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.twitter.chill.java.PackageRegistrar;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.SerializedLambda;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class KryoHelpers {
  private KryoHelpers() {}

  @SuppressWarnings("unchecked")
  public static <T> T roundTripSerialize(T obj) throws IOException {
    // Spark when using kryo serializer additionally includes serializers present
    // at https://github.com/twitter/chill. Presently complex serializers such as
    // serializers for unmodifiable java collections are picked from this library.
    Kryo kryo = new Kryo();

    // required for avoiding requirement of zero arg constructor
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

    // required for serializing and deserializing $$Lambda$ Anonymous Classes
    kryo.register(SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());

    PackageRegistrar.all().apply(kryo);

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
