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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TestHelpers;
import org.junit.jupiter.api.Test;

public class TestSerializableConfiguration {

  @Test
  public void kryoSerialization() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set("prefix.key1", "value1");
    configuration.set("prefix.key2", "value2");
    SerializableConfiguration conf = new SerializableConfiguration(configuration);
    SerializableConfiguration serialized = TestHelpers.KryoHelpers.roundTripSerialize(conf);

    assertThat(serialized.get().getPropsWithPrefix("prefix"))
        .isEqualTo(conf.get().getPropsWithPrefix("prefix"))
        .isEqualTo(configuration.getPropsWithPrefix("prefix"));
  }

  @Test
  public void javaSerialization() throws IOException, ClassNotFoundException {
    Configuration configuration = new Configuration();
    configuration.set("prefix.key1", "value1");
    configuration.set("prefix.key2", "value2");
    SerializableConfiguration conf = new SerializableConfiguration(configuration);
    SerializableConfiguration serialized = TestHelpers.roundTripSerialize(conf);

    assertThat(serialized.get().getPropsWithPrefix("prefix"))
        .isEqualTo(conf.get().getPropsWithPrefix("prefix"))
        .isEqualTo(configuration.getPropsWithPrefix("prefix"));
  }
}
