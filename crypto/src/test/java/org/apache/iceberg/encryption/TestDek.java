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

package org.apache.iceberg.encryption;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.util.conf.Properties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDek {
  byte[] encryptedDek;
  byte[] iv;
  Dek dek;

  @Before
  public void before() {
    Random random = new Random();
    int numBytes = 32;
    encryptedDek = new byte[numBytes];
    iv = new byte[numBytes];
    random.nextBytes(encryptedDek);
    random.nextBytes(iv);
    dek = new Dek(encryptedDek, iv);
    encryptedDek = Arrays.copyOf(encryptedDek, encryptedDek.length);
    iv = Arrays.copyOf(iv, iv.length);
  }

  @Test
  public void testDumpLoad() {
    Map<String, String> map = new HashMap<>();
    Properties conf = Properties.of(map);

    dek.dump(conf);
    Assert.assertArrayEquals(dek.encryptedDek(), encryptedDek);
    Assert.assertArrayEquals(dek.iv(), iv);

    Dek loaded = Dek.load(conf);
    Assert.assertEquals(loaded, dek);
  }
}
