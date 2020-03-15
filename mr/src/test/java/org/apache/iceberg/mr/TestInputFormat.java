package org.apache.iceberg.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;


public class TestInputFormat {
  IcebergInputFormat icebergInputFormat;

  public void test() {
    Configuration conf = new Configuration();
    conf = IcebergInputFormat.updateConf(conf, TestReadSupport.class).updatedConf();
    TaskAttemptContext context = new TaskAttemptContextImpl(new JobConf(conf), new TaskAttemptID());
    icebergInputFormat = new IcebergInputFormat();
    icebergInputFormat.getSplits(context);
  }

  public static final class TestReadSupport implements ReadSupport {
    @Override
    public <T> T addPartitionColumns(T row, Schema partitionSchema, PartitionSpec spec, StructLike partitionData) {
      return row;
    }
  }
}
