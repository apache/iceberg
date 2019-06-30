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

package org.apache.iceberg.avro;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestAvroIterable {
  private static final Logger logger = LoggerFactory.getLogger(TestAvroIterable.class);

  @Parameters
  public static Object[] parameters() {
    return new Object[] {Boolean.FALSE, Boolean.TRUE};
  }

  @Parameter
  public boolean shouldReuse;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private final Schema schema = new Schema(required(1, "id", Types.LongType.get()));
  private final List<Record> expected = RandomAvroData.generate(schema, 10, 0L);
  private AvroIterable<Record> avroIterable;

  @Before
  public void before() throws Exception {
    File file = temp.newFile();
    assertTrue("Delete should succeed", file.delete());
    try (FileAppender<Record> writer = Avro.write(Files.localOutput(file))
        .schema(schema)
        .named("test")
        .build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }
    avroIterable = Avro.read(Files.localInput(file))
        .project(schema)
        .reuseContainers(shouldReuse)
        .build();
  }

  @After
  public void after() throws Exception {
    avroIterable.close();
  }

  @Test
  public void getMetadata() throws Exception {
    Map<String, String> metadata = avroIterable.getMetadata();
    assertNotNull(metadata);
    assertEquals(metadata, avroIterable.getMetadata());
    Iterator<Record> iterator = avroIterable.iterator();
    assertTrue(iterator.hasNext());
    avroIterable.close();
    try {
      avroIterable.getMetadata();
      fail("Expecting " + IllegalStateException.class.getSimpleName());
    } catch (IllegalStateException e) {
      logger.debug("{}", avroIterable, e);
    }
  }

  @Test
  public void iterator() throws Exception {
    Iterator<Record> iterator = avroIterable.iterator();
    try {
      avroIterable.iterator();
      fail("Expecting " + IllegalStateException.class.getSimpleName());
    } catch (IllegalStateException e) {
      logger.debug("{}", avroIterable, e);
    }
    avroIterable.close();
    // possible bug in DataFileReader as hasNext() and next() do not fail after it was closed
    assertTrue(iterator.hasNext());
    Record record = iterator.next();
  }

  @Test
  public void close() throws Exception {
    avroIterable.getMetadata();
    avroIterable.iterator();
    avroIterable.getMetadata();
    avroIterable.close();
    try {
      avroIterable.getMetadata();
      fail("Expecting " + IllegalStateException.class.getSimpleName());
    } catch (IllegalStateException e) {
      logger.debug("{}", avroIterable, e);
    }
    try {
      avroIterable.iterator();
      fail();
    } catch (IllegalStateException e) {
      logger.debug("{}", avroIterable, e);
    }
  }

  @Test
  public void testFinalize() {
    try {
      avroIterable.iterator();
      avroIterable.finalize();
    } catch (Throwable throwable) {
      fail();
    }
  }
}
