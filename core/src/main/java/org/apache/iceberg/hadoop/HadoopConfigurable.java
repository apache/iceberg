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
package org.apache.iceberg.hadoop;

import java.util.function.Function;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * An interface that extends the Hadoop {@link Configurable} interface to offer better serialization
 * support for customizable Iceberg objects such as {@link org.apache.iceberg.io.FileIO}.
 *
 * <p>If an object is serialized and needs to use Hadoop configuration, it is recommended for the
 * object to implement this interface so that a serializable supplier of configuration can be
 * provided instead of an actual Hadoop configuration which is not serializable.
 */
public interface HadoopConfigurable extends Configurable {

  /**
   * Take a function that serializes Hadoop configuration into a supplier. An implementation is
   * supposed to pass in its current Hadoop configuration into this function, and the result can be
   * safely serialized for future use.
   *
   * @param confSerializer A function that takes Hadoop configuration and returns a serializable
   *     supplier of it.
   */
  void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer);
}
