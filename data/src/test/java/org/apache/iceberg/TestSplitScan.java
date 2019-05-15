package org.apache.iceberg;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSplitScan {
  private static final Configuration CONF = new Configuration();
  private static final HadoopCatalog HADOOP_CATALOG = new HadoopCatalog(CONF);

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
    table = HADOOP_CATALOG.createTable(
        new TableIdentifier(Namespace.empty(), tableLocation.toString()),
        SCHEMA,
        PartitionSpec.unpartitioned(),
        null);
    table.updateProperties()
        .set(TableProperties.SPLIT_SIZE, String.valueOf(SPLIT_SIZE))
        .commit();

    // With these number of records and the given SCHEMA
    // we can effectively write a file of approximate size 64 MB
    int numRecords = 2500000;
    expectedRecords = RandomGenericData.generate(SCHEMA, numRecords, 0L);
    File file = writeToFile(expectedRecords, format);

    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(expectedRecords.size())
        .withFileSizeInBytes(file.length())
        .withPath(file.toString())
        .withFormat(format)
        .build();

    table.newAppend().appendFile(dataFile).commit();
  }

  private File writeToFile(List<Record> records, FileFormat format) throws IOException {
    File file = temp.newFile();
    Assert.assertTrue(file.delete());

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
    return file;
  }
}
