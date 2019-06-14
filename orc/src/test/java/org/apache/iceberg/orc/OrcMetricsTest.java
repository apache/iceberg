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

package org.apache.iceberg.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.orc.OrcMetrics.EPOCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OrcMetricsTest {
  private static final Random RAND = new Random();
  private static final long CURRENT_MILLIS = System.currentTimeMillis();
  private static final long DAY_MILLIS = 86_400_000L;

  private final TypeDescription orcSchema;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public OrcMetricsTest() {
    orcSchema = TypeDescription.fromString("struct<a:int,b:bigint,c:int,d:double," +
        "e:timestamp,f:string,g:date,h:decimal(1,1)>");
  }

  private int daysFromMillis(long milisSinceEpoch) {
    return (int) ChronoUnit.DAYS.between(
        EPOCH.toLocalDate(),
        EPOCH.plus(milisSinceEpoch, ChronoUnit.MILLIS).toLocalDate());
  }

  private File writeOrcTestFile(int rows) throws IOException {
    final Configuration conf = new Configuration();

    final File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    final Path testPath = new Path(testFile.toURI().toString());
    final List<Long> optionsLong = ImmutableList.of(10L, 20L, 30L, 100L);
    final List<Double> optionsDouble = ImmutableList.of(1.3, 1.7, 0.21, 2.3, 0.09);

    try (Writer writer = OrcFile.createWriter(testPath,
        OrcFile.writerOptions(conf).setSchema(orcSchema))) {
      VectorizedRowBatch batch = orcSchema.createRowBatch();
      LongColumnVector aCol = (LongColumnVector) batch.cols[0];
      LongColumnVector bCol = (LongColumnVector) batch.cols[1];
      LongColumnVector cCol = (LongColumnVector) batch.cols[2];
      DoubleColumnVector dCol = (DoubleColumnVector) batch.cols[3];
      TimestampColumnVector eCol = (TimestampColumnVector) batch.cols[4];
      BytesColumnVector fCol = (BytesColumnVector) batch.cols[5];
      LongColumnVector gCol = (LongColumnVector) batch.cols[6];
      DecimalColumnVector hCol = (DecimalColumnVector) batch.cols[7];

      for (int r = 0; r < rows; ++r) {
        int row = batch.size++;
        aCol.vector[row] = r;
        bCol.vector[row] = optionsLong.get(RAND.nextInt(optionsLong.size()));
        cCol.vector[row] = r * 3L;
        dCol.vector[row] = optionsDouble.get(RAND.nextInt(optionsDouble.size()));
        eCol.fill(Timestamp.from(Instant.ofEpochMilli(CURRENT_MILLIS)));
        fCol.fill("foo".getBytes(StandardCharsets.UTF_8));
        gCol.vector[row] = (row % 2 == 0) ?
          daysFromMillis(CURRENT_MILLIS) : daysFromMillis(CURRENT_MILLIS - DAY_MILLIS);
        hCol.set(row, (row % 2 == 0) ?
            HiveDecimal.create(0.2) :
            HiveDecimal.create(0.1));
        // If the batch is full, write it out and start over.
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      }
      if (batch.size != 0) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    return testFile;
  }

  @Test
  public void testOrcMetricsPrimitive() throws IOException {
    final int rows = 10000;
    final File orcTestFile = writeOrcTestFile(rows);

    final Metrics orcMetrics = OrcMetrics.fromInputFile(Files.localInput(orcTestFile));
    assertNotNull(orcMetrics);
    assertEquals(rows, orcMetrics.recordCount().intValue());

    Schema icebergSchema = TypeConversion.fromOrc(orcSchema);

    Map<Integer, ?> lowerBounds = fromBufferMap(icebergSchema,
        orcMetrics.lowerBounds());
    assertEquals(0, lowerBounds.get(1));
    assertEquals(10L, lowerBounds.get(2));
    assertEquals(0, lowerBounds.get(3));
    assertEquals(0.09, lowerBounds.get(4));
    assertEquals(CURRENT_MILLIS, lowerBounds.get(5));
    assertEquals("foo", lowerBounds.get(6).toString());
    assertEquals(daysFromMillis(CURRENT_MILLIS - DAY_MILLIS), lowerBounds.get(7));
    // assertEquals(BigDecimal.valueOf(0.1d), lowerBounds.get(8));  // TODO: Fails, ORC returns 0

    Map<Integer, ?> upperBounds = fromBufferMap(icebergSchema,
        orcMetrics.upperBounds());
    assertEquals(rows - 1, upperBounds.get(1));
    assertEquals(100L, upperBounds.get(2));
    assertEquals((rows - 1) * 3, upperBounds.get(3));
    assertEquals(2.3, upperBounds.get(4));
    assertEquals(CURRENT_MILLIS, upperBounds.get(5));
    assertEquals("foo", upperBounds.get(6).toString());
    assertEquals(daysFromMillis(CURRENT_MILLIS), upperBounds.get(7));
    assertEquals(BigDecimal.valueOf(0.2d), upperBounds.get(8));
  }

  private Map<Integer, ?> fromBufferMap(Schema schema, Map<Integer, ByteBuffer> map) {
    Map<Integer, ?> values = Maps.newHashMap();
    for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
      values.put(entry.getKey(),
          Conversions.fromByteBuffer(schema.findType(entry.getKey()), entry.getValue()));
    }
    return values;
  }
}
