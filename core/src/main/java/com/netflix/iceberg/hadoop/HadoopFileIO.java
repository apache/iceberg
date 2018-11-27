package com.netflix.iceberg.hadoop;

import com.netflix.iceberg.FileIO;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class HadoopFileIO implements FileIO {
  private static final long serialVersionUID = 1L;

  private transient Configuration hadoopConf;

  public HadoopFileIO(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    hadoopConf.write(out);
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    hadoopConf = new Configuration(false);
    hadoopConf.readFields(in);
  }

  @Override
  public InputFile newInputFile(String path) {
    return HadoopInputFile.fromLocation(path, hadoopConf);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return HadoopOutputFile.fromPath(new Path(path), hadoopConf);
  }

  @Override
  public void deleteFile(String path) {
    Path toDelete = new Path(path);
    FileSystem fs = Util.getFS(toDelete, hadoopConf);
    try {
      fs.delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }
}
