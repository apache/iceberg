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

package org.apache.iceberg.gcp.gcs;

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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.InputFile;
import java.io.InputStream;
import java.util.HashMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark) // or Scope.Thread if GCSFileIO is thread-safe and state is per-thread
@Fork(value = 1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
/**
 * A benchmark that evaluates the performance of appending files to the table.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh
 *       -PjmhIncludeRegex=GCSIOBenchmark
 *       -PjmhOutputPath=benchmark/GCSIOBenchmark-benchmark.txt
 * </code>
 */
public class GCSIOBenchmark {

    private GCSFileIO gcsFileIO;
    private String testFilePathSmall; // gs://your-bucket/path/to/small-file.parquet
    private String testFilePathLarge; // gs://your-bucket/path/to/large-file.parquet
    private byte[] buffer;

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        gcsFileIO = new GCSFileIO();
        Map<String, String> properties = new HashMap<>();
        // Configure GCPProperties:
        // properties.put(GCPProperties.GCS_PROJECT_ID, "your-project-id");
        // properties.put(GCPProperties.GCS_SERVICE_ACCOUNT_KEY_FILE, "/path/to/your/key.json");
        // Or ensure Application Default Credentials are set up in the environment.
        gcsFileIO.initialize(properties);

        // Define paths to pre-existing test files in your GCS bucket
        testFilePathSmall = "gs://your-benchmark-bucket/test-data/small-file.parquet";
        testFilePathLarge = "gs://your-benchmark-bucket/test-data/large-file.parquet";

        // Create dummy files in GCS if they don't exist or ensure they are present
        // For a real benchmark, you'd upload actual Parquet/Avro/ORC files.
        // For simplicity here, this step is assumed to be done externally.

        buffer = new byte[8192]; // 8KB buffer for reading
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        if (gcsFileIO != null) {
            gcsFileIO.close();
        }
    }

    @Benchmark
    public InputFile newInputFileSmall() {
        return gcsFileIO.newInputFile(testFilePathSmall);
    }

    @Benchmark
    public InputFile newInputFileLarge() {
        return gcsFileIO.newInputFile(testFilePathLarge);
    }

    @Benchmark
    public long readSmallFile() throws IOException {
        InputFile inputFile = gcsFileIO.newInputFile(testFilePathSmall);
        long totalBytesRead = 0;
        try (InputStream stream = inputFile.newStream()) {
            int bytesRead;
            while ((bytesRead = stream.read(buffer)) != -1) {
                totalBytesRead += bytesRead;
            }
        }
        return totalBytesRead;
    }

    @Benchmark
    public long readLargeFile() throws IOException {
        InputFile inputFile = gcsFileIO.newInputFile(testFilePathLarge);
        long totalBytesRead = 0;
        try (InputStream stream = inputFile.newStream()) {
            int bytesRead;
            while ((bytesRead = stream.read(buffer)) != -1) {
                totalBytesRead += bytesRead;
            }
        }
        return totalBytesRead;
    }
}

