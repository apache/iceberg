package org.apache.iceberg.hadoop;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HadoopFileIO implements FileIO {

  private final SerializableConfiguration hadoopConf;

  public HadoopFileIO(Configuration hadoopConf) {
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
  }

  @Override
  public InputFile newInputFile(String path) {
    return HadoopInputFile.fromLocation(path, hadoopConf.get());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return HadoopOutputFile.fromPath(new Path(path), hadoopConf.get());
  }

  @Override
  public void deleteFile(String path) {
    Path toDelete = new Path(path);
    FileSystem fs = Util.getFS(toDelete, hadoopConf.get());
    try {
      fs.delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }
}
