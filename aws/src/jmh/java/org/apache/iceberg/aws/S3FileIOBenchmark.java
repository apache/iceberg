package org.apache.iceberg.aws;

import java.util.UUID;

import java.io.InputStream;
import java.io.OutputStream;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import java.nio.charset.Charset;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.annotations.OutputTimeUnit;


@State(Scope.Benchmark) // or Scope.Thread if S3FileIO is thread-safe and state is per-thread
@Fork(value = 1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
/**
 * A benchmark that evaluates the performance of reading files from S3 using S3FileIO.
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-aws:jmh
 *       -PjmhIncludeRegex=S3FileIOBenchmark
 *       -PjmhOutputPath=benchmark/S3FileIOBenchmark-benchmark.txt
 * </code>
 */
public class S3FileIOBenchmark {

    private S3FileIO s3FileIO;
    // private String testFilePathSmall;
    // private String testFilePathLarge;
    private byte[] buffer;

    @Setup
    public void before() {
        // testFilePathSmall = "s3://your-bucket/path/to/small-file.parquet";
        // testFilePathLarge = "s3://your-bucket/path/to/large-file.parquet";
        s3FileIO = new S3FileIO();
        Map<String, String> properties = new HashMap<>();
        // Configure S3Properties:
        properties.put(S3FileIOProperties.CROSS_REGION_ACCESS_ENABLED, "true");

        s3FileIO.initialize(properties);   
        buffer = new byte[8192]; // 8KB buffer for reading
        System.out.println("S3FileIO initialized with properties: " + properties);
        System.out.println("Buffer size: " + buffer.length + " bytes");
    }

    @TearDown
    public void after() {
        if (s3FileIO != null) {
            s3FileIO.close();
        }
    }
    private final String BASE_PATH = "s3://shubham-iceberg-testing/benchmark_write_table/";
    // private final String SUCCESS_PATH = "s3://shubham-iceberg-testing/";
    private final int NUM_FILES = 100; // Example for N
    private final int NUM_RECORDS = 25_000_000; // Example for number of records per file
    private final byte[] RECORD_DATA = "sample,record,data1\n".getBytes(Charset.defaultCharset());
    
    @Benchmark
    public void writeNFilesAndSuccessS3() throws IOException {
        String runID = UUID.randomUUID().toString();
        String writePath = BASE_PATH + runID + "/";
        // int nFiles = 10; // Number of files to write
        for (int i = 0; i < NUM_FILES; i++) {
            String filePath = writePath + "data_part_0000" + i + ".csv";
            OutputFile outputFile = s3FileIO.newOutputFile(filePath);
            try (OutputStream outputStream = outputFile.createOrOverwrite()) {
                for (int j = 0; j < NUM_RECORDS; j++) { // Example for number of records per file
                    outputStream.write(RECORD_DATA);
                }
            } catch (IOException e) {
                System.err.println("Error writing file " + filePath + ": " + e.getMessage());
            }
        }
    }

    // @Benchmark
    // public long readSmallFile() {
    //     InputFile inputFile = s3FileIO.newInputFile(testFilePathSmall);    
    //     long totalBytesRead = 0;
    //     try (InputStream stream = inputFile.newStream()) {
    //         int bytesRead;
    //         while ((bytesRead = stream.read(buffer)) != -1) {
    //             totalBytesRead += bytesRead;
    //         }
    //     } catch (IOException e) {
    //         System.err.println("Error reading small file: " + e.getMessage());
    //     }
    //     return totalBytesRead;
    // }

    // @Benchmark
    // public long readLargeFile() {
    //     InputFile inputFile = s3FileIO.newInputFile(testFilePathLarge);
    //     long totalBytesRead = 0;
    //     try (InputStream stream = inputFile.newStream()) {
    //         int bytesRead;
    //         while ((bytesRead = stream.read(buffer)) != -1) {
    //             totalBytesRead += bytesRead;
    //         }
    //     } catch (IOException e) {
    //         System.err.println("Error reading large file: " + e.getMessage());
    //     }
    //     return totalBytesRead;
    // }
}
