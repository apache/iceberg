package org.apache.iceberg.mr;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.RandomAvroData;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


public class TestIcebergInputFormat {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private Table table;
  private File tableLocation;

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "date", Types.StringType.get()));

  private static final PartitionSpec PARTITION_BY_DATE = PartitionSpec
      .builderFor(SCHEMA)
      .identity("date")
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  IcebergInputFormat<GenericData.Record> icebergInputFormat;

  @Test
  public void test() throws IOException, InterruptedException {
    tableLocation = new File(temp.newFolder(), "table");
    Table table = TABLES.create(SCHEMA, tableLocation.toString());
    List<GenericData.Record> records = RandomAvroData.generate(SCHEMA, 5, 0L);
    File file = temp.newFile();
    Assert.assertTrue(file.delete());
    try (FileAppender<GenericData.Record> appender = Avro.write(Files.localOutput(file))
                                                         .schema(SCHEMA)
                                                         .named("avro")
                                                         .build()) {
      appender.addAll(records);
    }

    DataFile dataFile = DataFiles.builder(PARTITION_BY_DATE)
                                 .withRecordCount(records.size())
                                 .withFileSizeInBytes(file.length())
                                 .withPath(file.toString())
                                 .withFormat("avro")
                                 .build();

    table.newAppend().appendFile(dataFile).commit();

    Configuration conf = new Configuration();
    conf = IcebergInputFormat
        .updateConf(conf, tableLocation.getAbsolutePath(), TestReadSupport.class)
        .updatedConf();
    TaskAttemptContext context = new TaskAttemptContextImpl(new JobConf(conf), new TaskAttemptID());
    icebergInputFormat = new IcebergInputFormat<>();
    List<InputSplit> splits = icebergInputFormat.getSplits(context);
    final RecordReader<Void, GenericData.Record> recordReader = icebergInputFormat.createRecordReader(splits.get(0), context);
    recordReader.initialize(splits.get(0), context);
    while (recordReader.nextKeyValue()) {
      System.out.println(recordReader.getCurrentValue());
    }
  }

  public static class TestReadSupport implements ReadSupport<GenericData.Record> {
    @Override
    public GenericData.Record withPartitionColumns(
        GenericData.Record row, Schema partitionSchema, PartitionSpec spec, StructLike partitionData) {
      return row;
    }
  }
}
