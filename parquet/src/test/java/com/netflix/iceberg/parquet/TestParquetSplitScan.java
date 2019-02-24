package com.netflix.iceberg.parquet;

import com.google.common.collect.FluentIterable;
import com.netflix.iceberg.*;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.types.Types;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static com.netflix.iceberg.types.Types.NestedField.required;
import static org.apache.avro.generic.GenericData.Record;

public class TestParquetSplitScan {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final long SPLIT_SIZE = 16 * 1024 * 1024;

  private Schema schema = new Schema(
      required(0, "id", Types.IntegerType.get()),
      required(1, "data", Types.StringType.get())
  );

  private Table table;
  private File tableLocation;
  private int noOfRecords;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    tableLocation = new File(temp.newFolder(), "table");
    setupTable();
  }

  @Test
  public void test() {
    int nTasks = 0;
    int nRecords = 0;
    CloseableIterable<CombinedScanTask> tasks = table.newScan().planTasks();
    for (CombinedScanTask task : tasks) {
      Iterable<Record> records = records(table, schema, task);
      for (Record record : records) {
        Assert.assertEquals("Record " + record.get("id") + " is not read in order", nRecords, record.get("id"));
        nRecords += 1;
      }
      nTasks += 1;
    }

    Assert.assertEquals("Total number of records read should match " + nRecords, nRecords, noOfRecords);
    Assert.assertEquals("There should be 4 tasks created since file size is ~ 64 mb and split size ~ 16", 4, nTasks);
  }

  private void setupTable() throws IOException {
    table = TABLES.create(schema, tableLocation.toString());
    table.updateProperties()
        .set(TableProperties.SPLIT_SIZE, String.valueOf(SPLIT_SIZE))
        .commit();

    File file = temp.newFile();
    file.delete();
    noOfRecords = addRecordsToFile(file);

    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(noOfRecords)
        .withFileSizeInBytes(file.length())
        .withPath(file.toString())
        .withFormat(FileFormat.AVRO)
        .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  private int addRecordsToFile(File file) throws IOException {
    int nRecords = 1600000;
    try (FileAppender<Record> writer = Parquet.write(Files.localOutput(file))
        .schema(schema)
        .set(TableProperties.PARQUET_COMPRESSION, "uncompressed")
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(SPLIT_SIZE))
        .build()) {
      for (int i = 0; i < nRecords; i++) {
        GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "table"));
        Record record = builder
            .set("id", i)
            .set("data", "abcdefghijklmnopqrstuvwxyz_" + i)
            .build();
        writer.add(record);
      }
    }
    return nRecords;
  }

  private static Iterable<Record> records(Table table, Schema schema, CombinedScanTask task) {
    return FluentIterable
        .from(task.files())
        .transformAndConcat(ft -> records(table, schema, ft));
  }

  private static Iterable<Record> records(Table table, Schema schema, FileScanTask task) {
    InputFile input = table.io().newInputFile(task.file().path().toString());
    return Parquet.read(input)
        .project(schema)
        .split(task.start(), task.length())
        .build();
  }
}
