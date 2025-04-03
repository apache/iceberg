package org.apache.iceberg.parquet;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// Just a test I made to convert Parquet files
public class ParquetConverterTest {
  // 512MiB
  private static final long ROW_GROUP_SIZE = 512 * 1024 * 1024;

  // 1GiB
  private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024;

  private static final Map<String, List<String>> DECIMALS =
      Map.of(
          "customer",
          List.of(),
          "lineitem",
          List.of("l_quantity", "l_extendedprice", "l_discount", "l_tax"),
          "nation",
          List.of(),
          "orders",
          List.of("o_totalprice"),
          "partsupp",
          List.of("ps_supplycost"),
          "part",
          List.of("p_retailprice"),
          "region",
          List.of(),
          "supplier",
          List.of("s_acctbal"));

  @ParameterizedTest
  @ValueSource(
      strings = {
        "customer",
        "lineitem",
        "nation",
        "orders",
        "partsupp",
        "part",
        "region",
        "supplier"
      })
  public void rewriteMyFiles(String tableName) throws Exception {
    Configuration conf = new Configuration();
    Path inputPath =
        new Path("/Volumes/DuffyT7/Datasets/TPCH/sf=100/unpartitioned/" + tableName + ".parquet");
    Path outputDir = new Path("/Volumes/DuffyT7/Datasets/TPCH/sf=100/partitioned/");
    Path tmpDir = new Path("/Volumes/Code/tmp");

    List<String> decimalCols = DECIMALS.get(tableName);

    // Create AvroParquetReader to read GenericRecords.
    ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(inputPath).withConf(conf).build();

    GenericRecord firstRecord = reader.read();
    if (firstRecord == null) {
      System.err.println("Input file is empty.");
      reader.close();
      return;
    }
    Schema schema = firstRecord.getSchema();
    Schema outSchema = replaceDecimalWithFloat64(schema, decimalCols);
    firstRecord = convertRecord(firstRecord, decimalCols);

    // Buffer to accumulate records for one row group.
    List<GenericRecord> buffer = new ArrayList<>();
    buffer.add(firstRecord);

    int fileIndex = 0;
    long currentFileSize = 0;
    Path currentOutPath = new Path(outputDir, tableName + "_" + fileIndex + ".parquet");
    ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(currentOutPath)
            .withSchema(outSchema)
            .withConf(conf)
            .withCompressionCodec(CompressionCodecName.ZSTD)
            .withRowGroupSize(ROW_GROUP_SIZE)
            .build();

    // Define a buffer threshold (adjust as needed).
    final int BUFFER_THRESHOLD = 10_000;
    GenericRecord record;
    while ((record = reader.read()) != null) {
      record = convertRecord(record, decimalCols);
      buffer.add(record);
      if (buffer.size() >= BUFFER_THRESHOLD) {
        // Estimate size of the buffered row group.
        long groupSize = estimateRowGroupSize(buffer, schema, conf, ROW_GROUP_SIZE, tmpDir);
        // If adding this group would exceed the max file size, start a new file.
        if (currentFileSize + groupSize > MAX_FILE_SIZE) {
          writer.close();
          fileIndex++;
          currentOutPath = new Path(outputDir, tableName + "_" + fileIndex + ".parquet");
          writer =
              AvroParquetWriter.<GenericRecord>builder(currentOutPath)
                  .withSchema(schema)
                  .withConf(conf)
                  .withCompressionCodec(CompressionCodecName.ZSTD)
                  .withRowGroupSize(ROW_GROUP_SIZE)
                  .build();
          currentFileSize = 0;
        }
        // Write buffered records.
        for (GenericRecord rec : buffer) {
          writer.write(rec);
        }
        currentFileSize += groupSize;
        buffer.clear();
      }
    }

    // Write any remaining records.
    if (!buffer.isEmpty()) {
      long groupSize = estimateRowGroupSize(buffer, schema, conf, ROW_GROUP_SIZE, tmpDir);
      if (currentFileSize + groupSize > MAX_FILE_SIZE) {
        writer.close();
        fileIndex++;
        currentOutPath = new Path(outputDir, tableName + "_" + fileIndex + ".parquet");
        writer =
            AvroParquetWriter.<GenericRecord>builder(currentOutPath)
                .withSchema(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withRowGroupSize(ROW_GROUP_SIZE)
                .build();
      }
      for (GenericRecord rec : buffer) {
        writer.write(rec);
      }
    }
    writer.close();
    reader.close();
  }

  private static long estimateRowGroupSize(
      List<GenericRecord> records,
      Schema schema,
      Configuration conf,
      long rowGroupSizeBytes,
      Path tempDir)
      throws IOException {
    String tempFileName = "temp_" + System.nanoTime() + ".parquet";
    Path tempPath = new Path(tempDir, tempFileName);
    ParquetWriter<GenericRecord> tempWriter =
        AvroParquetWriter.<GenericRecord>builder(tempPath)
            .withSchema(schema)
            .withConf(conf)
            .withCompressionCodec(CompressionCodecName.ZSTD)
            .withRowGroupSize(rowGroupSizeBytes)
            .build();
    for (GenericRecord rec : records) {
      tempWriter.write(rec);
    }
    tempWriter.close();

    FileSystem fs = FileSystem.get(conf);
    long size = fs.getFileStatus(tempPath).getLen();
    fs.delete(tempPath, false);
    return size;
  }

  private static GenericRecord convertRecord(GenericRecord oldRecord, List<String> decimalFields) {
    Schema oldSchema = oldRecord.getSchema();
    Schema newSchema = replaceDecimalWithFloat64(oldSchema, decimalFields);
    GenericRecord newRecord = new GenericData.Record(newSchema);
    // Assuming scale=2
    for (Schema.Field field : oldRecord.getSchema().getFields()) {
      Object value = oldRecord.get(field.name());
      if (decimalFields.contains(field.name()) && value != null) {
        long longValue = (Long) value;
        BigDecimal bd = BigDecimal.valueOf(longValue, 2);
        newRecord.put(field.name(), bd.doubleValue());
      } else {
        newRecord.put(field.name(), value);
      }
    }
    return newRecord;
  }

  private static Schema replaceDecimalWithFloat64(Schema oldSchema, List<String> decimalFields) {
    Schema newSchema =
        Schema.createRecord(
            oldSchema.getName(), oldSchema.getDoc(), oldSchema.getNamespace(), false);
    List<Schema.Field> newFields = new ArrayList<>();
    for (Schema.Field field : oldSchema.getFields()) {
      Schema newFieldType = field.schema();
      if (decimalFields.contains(field.name())) {
        newFieldType = Schema.create(Schema.Type.DOUBLE);
      }
      Schema.Field newField = new Schema.Field(field.name(), newFieldType, field.doc(), null);
      newFields.add(newField);
    }
    newSchema.setFields(newFields);
    return newSchema;
  }
}
