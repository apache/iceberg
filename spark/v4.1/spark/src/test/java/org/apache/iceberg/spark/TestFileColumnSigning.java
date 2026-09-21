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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PreSignedUrlTestServer;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RemoteSignerServlet;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** What consuming a {@code file} column in Spark costs in signing requests. */
public class TestFileColumnSigning {

  private static final Types.FileType FILE_TYPE = Types.FileType.of(1);
  private static final int GROUP = 4;
  private static final int ROWS = 12;
  private static final int OBJECTS = 6;
  private static final String FILE_COLUMN = "file";
  private static final String BUCKET = "blobs";
  private static final String PACKED = "captions.txt";
  private static final String SIGN_ENDPOINT = "v1/namespaces/ns/tables/t/sign";

  private static SparkSession spark;
  private static Server catalog;
  private static SigningServlet servlet;
  private static PreSignedUrlTestServer store;

  @TempDir private static Path temp;

  private static final StructType FILE_SCHEMA = (StructType) SparkSchemaUtil.convert(FILE_TYPE);

  private static final StructType ROW_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField(FILE_COLUMN, FILE_SCHEMA, true)
          });

  @BeforeAll
  public static void startServices() throws Exception {
    store = new PreSignedUrlTestServer(temp.resolve("store"));
    servlet = new SigningServlet();
    catalog = new Server(0);
    ServletContextHandler context = new ServletContextHandler();
    context.addServlet(new ServletHolder(servlet), "/*");
    catalog.setHandler(context);
    catalog.start();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("file-value-reader")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.metrics.conf.*.sink.servlet.class",
                "org.apache.iceberg.spark.DummyMetricsServlet")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
  }

  @AfterAll
  public static void stopServices() throws Exception {
    if (spark != null) {
      spark.stop();
      spark = null;
    }

    catalog.stop();
    store.close();
  }

  @BeforeEach
  public void resetCounters() {
    servlet.resetSignCounters();
  }

  @Test
  public void testInClusterInference() throws IOException {
    Dataset<Row> values = localValues(OBJECTS);

    List<Boolean> results = values.mapPartitions(IN_CLUSTER, Encoders.BOOLEAN()).collectAsList();

    assertThat(results).hasSize(ROWS);
    assertThat(results.stream().filter(hit -> hit).count()).isEqualTo(ROWS / 2);
  }

  @Test
  public void testBatchedSigningIsOneRoundTripPerGroup() {
    Dataset<Row> values = signedValues(ROWS);

    assertThat(values.mapPartitions(handOff(true), Encoders.BOOLEAN()).collectAsList())
        .hasSize(ROWS);
    assertThat(servlet.signRoundTrips()).isEqualTo(4);
    assertThat(servlet.signedLocations()).isEqualTo(ROWS);
    report(ROWS);
  }

  @Test
  public void testSingularSigningIsOneRoundTripPerLocation() {
    Dataset<Row> values = signedValues(ROWS);

    assertThat(values.mapPartitions(handOff(false), Encoders.BOOLEAN()).collectAsList())
        .hasSize(ROWS);
    assertThat(servlet.signRoundTrips()).isEqualTo(ROWS);
    assertThat(servlet.signedLocations()).isEqualTo(ROWS);
    report(ROWS);
  }

  @Test
  public void testOffsetReadOverPreSignedUrlIsOneRangeRequest() throws IOException {
    int object = 1;
    long offset = offsetOf(object);
    long size = content(object).length;
    store.put(PACKED, packedContent(OBJECTS));
    Row row =
        spark
            .createDataFrame(
                List.of(row(0, "s3://" + BUCKET + "/" + PACKED, offset, size)), ROW_SCHEMA)
            .collectAsList()
            .get(0);
    Row value = row.getStruct(row.fieldIndex(FILE_COLUMN));

    try (SigningFileIO io = fileIO(properties(false))) {
      RemoteSignResponse signed = io.preSign(value.<String>getAs("uri"));
      int requestsBefore = store.ranges().size();

      byte[] bytes = referent(value, io.newInputFile(signed.uri().toString()));

      assertThat(bytes).isEqualTo(content(object));
      assertThat(store.ranges().subList(requestsBefore, store.ranges().size()))
          .containsExactly("bytes=" + offset + "-" + (offset + size - 1));
    }
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSigningAfterSerialization(
      TestHelpers.RoundTripSerializer<SigningFileIO> roundTripSerializer) throws Exception {
    String key = "blob-0.txt";
    store.put(key, content(0));
    List<String> locations = List.of("s3://" + BUCKET + "/" + key);

    try (SigningFileIO io = fileIO(properties(true))) {
      io.preSign(locations);

      try (SigningFileIO executorCopy = roundTripSerializer.apply(io)) {
        assertThat(classify(Lists.newArrayList(executorCopy.preSign(locations).values())))
            .containsExactly(false);
        assertThat(servlet.signRoundTrips()).isEqualTo(2);
      }
    }
  }

  private static final MapPartitionsFunction<Row, Boolean> IN_CLUSTER =
      rows -> {
        FileIO io = new HadoopFileIO(new Configuration());
        List<Boolean> out = Lists.newArrayList();
        for (List<Row> group : groups(rows)) {
          for (Row value : group) {
            InputFile file = io.newInputFile(value.<String>getAs("uri"));
            out.add(hasEmoji(referent(value, file)));
          }
        }

        return out.iterator();
      };

  private static MapPartitionsFunction<Row, Boolean> handOff(boolean batched) {
    Map<String, String> properties = properties(batched);
    return rows -> {
      try (SigningFileIO io = fileIO(properties)) {
        List<Boolean> out = Lists.newArrayList();
        for (List<Row> group : groups(rows)) {
          List<String> uris =
              group.stream()
                  .map(value -> value.<String>getAs("uri"))
                  .distinct()
                  .collect(Collectors.toList());
          Map<String, RemoteSignResponse> signed = io.preSign(uris);
          out.addAll(
              classify(
                  group.stream()
                      .map(value -> signed.get(value.<String>getAs("uri")))
                      .collect(Collectors.toList())));
        }

        return out.iterator();
      }
    };
  }

  private static SigningFileIO fileIO(Map<String, String> properties) {
    SigningFileIO io = new SigningFileIO();
    io.initialize(properties);
    return io;
  }

  private static Map<String, String> properties(boolean batched) {
    ImmutableMap.Builder<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, catalog.getURI().toString())
            .put(RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT, SIGN_ENDPOINT);
    if (batched) {
      properties.put(RESTCatalogProperties.REMOTE_SIGNING_BATCH_SUPPORTED, "true");
    }

    return properties.build();
  }

  private static List<Boolean> classify(List<RemoteSignResponse> references) {
    List<Boolean> results = Lists.newArrayList();
    for (RemoteSignResponse reference : references) {
      HttpRequest.Builder request = HttpRequest.newBuilder(reference.uri()).GET();
      reference.headers().forEach((name, values) -> values.forEach(v -> request.header(name, v)));
      try {
        HttpResponse<byte[]> response =
            HttpClient.newHttpClient()
                .send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
        assertThat(response.statusCode()).isEqualTo(200);
        results.add(hasEmoji(response.body()));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }

    return results;
  }

  private static void report(int objects) {
    System.out.printf(
        "%d rows over %d distinct objects: %d signing round trips for %d locations%n",
        ROWS, objects, servlet.signRoundTrips(), servlet.signedLocations());
  }

  private static List<List<Row>> groups(java.util.Iterator<Row> rows) {
    List<List<Row>> groups = Lists.newArrayList();
    List<Row> current = Lists.newArrayList();
    while (rows.hasNext()) {
      Row row = rows.next();
      current.add(row.getStruct(row.fieldIndex(FILE_COLUMN)));
      if (current.size() == GROUP) {
        groups.add(current);
        current = Lists.newArrayList();
      }
    }

    if (!current.isEmpty()) {
      groups.add(current);
    }

    return groups;
  }

  private static byte[] referent(Row value, InputFile file) throws IOException {
    Long declaredOffset = value.getAs("offset");
    Long declaredSize = value.getAs("size");
    long offset = declaredOffset == null ? 0 : declaredOffset;
    long size = declaredSize != null ? declaredSize : file.getLength() - offset;

    byte[] bytes = new byte[Math.toIntExact(size)];
    try (SeekableInputStream stream = file.newStream()) {
      if (stream instanceof RangeReadable) {
        ((RangeReadable) stream).readFully(offset, bytes);
      } else {
        stream.seek(offset);
        IOUtil.readFully(stream, bytes, 0, bytes.length);
      }
    }

    return bytes;
  }

  private static boolean hasEmoji(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8).codePoints().anyMatch(cp -> cp >= 0x1F300);
  }

  private static byte[] content(int object) {
    String text = object % 2 == 0 ? "a plain caption " + object : "a caption 😀 " + object;
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] packedContent(int objects) {
    ByteArrayOutputStream packed = new ByteArrayOutputStream();
    for (int object = 0; object < objects; object += 1) {
      packed.writeBytes(content(object));
    }

    return packed.toByteArray();
  }

  private static long offsetOf(int object) {
    long offset = 0;
    for (int before = 0; before < object; before += 1) {
      offset += content(before).length;
    }

    return offset;
  }

  private Dataset<Row> localValues(int objects) throws IOException {
    File packed = temp.resolve(PACKED).toFile();
    if (!packed.exists()) {
      Files.write(packed.toPath(), packedContent(objects));
    }

    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < ROWS; i += 1) {
      int object = i % objects;
      rows.add(row(i, packed.getAbsolutePath(), offsetOf(object), content(object).length));
    }

    return spark.createDataFrame(rows, ROW_SCHEMA).repartition(2);
  }

  private Dataset<Row> signedValues(int objects) {
    List<Row> rows = Lists.newArrayList();
    for (int i = 0; i < ROWS; i += 1) {
      int object = i % objects;
      byte[] content = content(object);
      String key = "blob-" + object + ".txt";
      store.put(key, content);
      rows.add(row(i, "s3://" + BUCKET + "/" + key, null, content.length));
    }

    return spark.createDataFrame(rows, ROW_SCHEMA).repartition(2);
  }

  private static Row row(int id, String uri, Long offset, long size) {
    return RowFactory.create(id, RowFactory.create(uri, offset, size, "text/plain", null, null));
  }

  static class SigningServlet extends RemoteSignerServlet {
    SigningServlet() {
      super(SIGN_ENDPOINT);
    }

    @Override
    protected RemoteSignResponse signRequest(RemoteSignRequest request) {
      String key = request.uri().toString().substring(("s3://" + BUCKET + "/").length());
      return ImmutableRemoteSignResponse.builder().uri(URI.create(store.url(key))).build();
    }
  }
}
