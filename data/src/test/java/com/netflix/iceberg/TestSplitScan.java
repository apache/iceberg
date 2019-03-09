package com.netflix.iceberg;

import com.google.common.collect.Lists;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.data.IcebergGenerics;
import com.netflix.iceberg.data.RandomGenericData;
import com.netflix.iceberg.data.Record;
import com.netflix.iceberg.data.avro.DataWriter;
import com.netflix.iceberg.data.parquet.GenericParquetWriter;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static com.netflix.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSplitScan {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final long SPLIT_SIZE = 16 * 1024 * 1024;

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get())
  );

  private Table table;
  private File tableLocation;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private List<Record> expectedRecords;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][]{
        new Object[]{"parquet"},
        new Object[]{"avro"}
    };
  }

  private final FileFormat format;

  public TestSplitScan(String format) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void setup() throws IOException {
    tableLocation = new File(temp.newFolder(), "table");
    setupTable();
  }

  @Test
  public void test() {
    Assert.assertEquals(
        "There should be 4 tasks created since file size is approximately close to 64MB and split size 16MB",
        4, Lists.newArrayList(table.newScan().planTasks()).size());
    List<Record> records = Lists.newArrayList(IcebergGenerics.read(table).build());
    Assert.assertEquals(expectedRecords.size(), records.size());
    for (int i = 0; i < expectedRecords.size(); i++) {
      Assert.assertEquals(expectedRecords.get(i), records.get(i));
    }
  }

  private void setupTable() throws IOException {
    table = TABLES.create(SCHEMA, tableLocation.toString());
    table.updateProperties()
        .set(TableProperties.SPLIT_SIZE, String.valueOf(SPLIT_SIZE))
        .commit();

    File file = temp.newFile();
    Assert.assertTrue(file.delete());
    expectedRecords = writeFile(file, format);

    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(expectedRecords.size())
        .withFileSizeInBytes(file.length())
        .withPath(file.toString())
        .withFormat(format)
        .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  private List<Record> writeFile(File file, FileFormat format) throws IOException {
    // With these number of records and the given SCHEMA
    // we can effectively write a file of approximate size 64 MB
    int numRecords = 2500000;
    List<Record> records = RandomGenericData.generate(SCHEMA, numRecords, 0L);
    switch (format) {
      case AVRO:
        try (FileAppender<Record> appender = Avro.write(Files.localOutput(file))
            .schema(SCHEMA)
            .createWriterFunc(DataWriter::create)
            .named(format.name())
            .build()) {
          appender.addAll(records);
        }
        break;
      case PARQUET:
        try (FileAppender<Record> appender = Parquet.write(Files.localOutput(file))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .named(format.name())
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(SPLIT_SIZE))
            .build()) {
          appender.addAll(records);
        }
        break;
      default:
        throw new UnsupportedOperationException("Cannot write format: " + format);
    }
    return records;
  }
}
