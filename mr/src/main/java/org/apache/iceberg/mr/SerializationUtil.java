package org.apache.iceberg.mr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.iceberg.exceptions.RuntimeIOException;


public class SerializationUtil {

  private SerializationUtil() {
  }

  public static byte[] serializeToBytes(Object obj) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(baos);
        ObjectOutputStream oos = new ObjectOutputStream(gos)) {
      oos.writeObject(obj);
      return baos.toByteArray();
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserializeFromBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        GZIPInputStream gis = new GZIPInputStream(bais);
        ObjectInputStream ois = new ObjectInputStream(gis)) {
      return (T) ois.readObject();
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not read object ", e);
    }
  }

  public static String serializeToBase64(Object obj) throws IOException {
    byte[] bytes = serializeToBytes(obj);
    return new String(Base64.getMimeEncoder().encode(bytes), StandardCharsets.UTF_8);
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserializeFromBase64(String base64) {
    if (base64 == null) {
      return null;
    }
    byte[] bytes = Base64.getMimeDecoder().decode(base64.getBytes(StandardCharsets.UTF_8));
    return deserializeFromBytes(bytes);
  }
}
