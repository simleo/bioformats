/*
 * #%L
 * Common package for I/O and related utilities
 * %%
 * Copyright (C) 2005 - 2013 Open Microscopy Environment:
 *   - Board of Regents of the University of Wisconsin-Madison
 *   - Glencoe Software, Inc.
 *   - University of Dundee
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */

package loci.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * A wrapper for Hadoop I/O streams that implements the IRandomAccess interface.
 *
 * <dl><dt><b>Source code:</b></dt>
 * <dd><a href="http://trac.openmicroscopy.org.uk/ome/browser/bioformats.git/components/common/src/loci/common/DFSHandle.java">Trac</a>,
 * <a href="http://git.openmicroscopy.org/?p=bioformats.git;a=blob;f=components/common/src/loci/common/DFSHandle.java;hb=HEAD">Gitweb</a></dd></dl>
 *
 * @see IRandomAccess
 *
 * @author Simone Leo simone.leo at crs4.it
 */
public class DFSHandle implements IRandomAccess {

  // -- Fields --

  /** The FileSystem instance this handle belongs to. */
  protected FileSystem fs;

  /** Backing Path instance. */
  protected Path path;

  /** Backing input stream. */
  protected FSDataInputStream stream;

  /** Backing output stream. */
  protected FSDataOutputStream outStream;

  /** Byte ordering of this file. */
  protected ByteOrder order;

  // -- Constructors --

  /**
   * Create a new DFSHandle from a path with the specified name.
   * @param bufferSize the size of the buffer to be used. If negative,
   * Hadoop's configured / default buffer size will be used.
   */
  public DFSHandle(String name, String mode, int bufferSize)
      throws IOException {
    Configuration conf = new Configuration();
    fs = FileSystem.get(URI.create(name), conf);
    path = new Path(name);
    // TODO: support more signatures for create()
    if (mode.equals("r")) {
      stream = (bufferSize < 0) ? fs.open(path) : fs.open(path, bufferSize);
    }
    else if (mode.equals("w")) {
      if (bufferSize < 0) {
        outStream = fs.create(path);
      }
      else {
        boolean overwrite = true;
        outStream = fs.create(path, overwrite, bufferSize);
      }
    }
    else {
      throw new IllegalArgumentException(
        String.format("%s mode not in supported modes ('r', 'w')", mode));
    }
    order = ByteOrder.BIG_ENDIAN;
  }

  /**
   * Create a new DFSHandle from a path with the specified name.
   */
  public DFSHandle(String name, String mode) throws IOException {
    this(name, mode, -1);
  }

  private short adaptOrder(short x) {
    return order.equals(ByteOrder.LITTLE_ENDIAN) ? DataTools.swap(x) : x;
  }

  private char adaptOrder(char x) {
    return order.equals(ByteOrder.LITTLE_ENDIAN) ? DataTools.swap(x) : x;
  }

  private int adaptOrder(int x) {
    return order.equals(ByteOrder.LITTLE_ENDIAN) ? DataTools.swap(x) : x;
  }

  private long adaptOrder(long x) {
    return order.equals(ByteOrder.LITTLE_ENDIAN) ? DataTools.swap(x) : x;
  }

  private float adaptOrder(float x) {
    return order.equals(ByteOrder.LITTLE_ENDIAN) ? DataTools.swap(x) : x;
  }

  private double adaptOrder(double x) {
    return order.equals(ByteOrder.LITTLE_ENDIAN) ? DataTools.swap(x) : x;
  }

  // -- DFSHandle API methods --

  /** Get the FileSystem this handle belongs to. */
  public FileSystem getFileSystem() { return fs; }

  // -- IRandomAccess API methods --

  /* @see IRandomAccess.close() */
  public void close() throws IOException {
    if (stream != null) stream.close();
    if (outStream != null) outStream.close();
    stream = null;
    outStream = null;
  }

  /* @see IRandomAccess.getFilePointer() */
  public long getFilePointer() throws IOException {
    if (stream != null) return stream.getPos();
    else return outStream.getPos();
  }

  /* @see IRandomAccess.length() */
  public long length() throws IOException {
    return fs.getFileStatus(path).getLen();
  }

  /* @see IRandomAccess.read(byte[]) */
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  /* @see IRandomAccess.read(byte[], int, int) */
  public int read(byte[] b, int off, int len) throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    int readLength = stream.read(b, off, len);  // -1 if EOF has been reached
    return readLength == -1 ? 0 : readLength;
  }

  /* @see IRandomAccess.read(ByteBuffer) */
  public int read(ByteBuffer buffer) throws IOException {
    return read(buffer, 0, buffer.capacity());
  }

  /* @see IRandomAccess.read(ByteBuffer, int, int) */
  public int read(ByteBuffer buffer, int off, int len) throws IOException {
    byte[] b = new byte[len];
    int n = read(b);
    buffer.put(b, off, len);
    return n;
  }

  /* @see IRandomAccess.seek(long) */
  public void seek(long pos) throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    stream.seek(pos);
  }

  /* @see IRandomAccess.write(ByteBuffer) */
  public void write(ByteBuffer buf) throws IOException {
    write(buf, 0, buf.capacity());
  }

  /* @see IRandomAccess.write(ByteBuffer, int, int) */
  public void write(ByteBuffer buf, int off, int len) throws IOException {
    buf.position(off);
    if (buf.hasArray()) {
      write(buf.array(), off, len);
    }
    else {
      byte[] b = new byte[len];
      buf.get(b);
      write(b);
    }
  }

  // -- DataInput API methods --

  /* @see java.io.DataInput.readBoolean() */
  public boolean readBoolean() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return stream.readBoolean();
  }

  /* @see java.io.DataInput.readByte() */
  public byte readByte() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return stream.readByte();
  }

  /* @see java.io.DataInput.readChar() */
  public char readChar() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return stream.readChar();
  }

  /* @see java.io.DataInput.readDouble() */
  public double readDouble() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return adaptOrder(stream.readDouble());
  }

  /* @see java.io.DataInput.readFloat() */
  public float readFloat() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return adaptOrder(stream.readFloat());
  }

  /* @see java.io.DataInput.readFully(byte[]) */
  public void readFully(byte[] b) throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    stream.readFully(b);
  }

  /* @see java.io.DataInput.readFully(byte[], int, int) */
  public void readFully(byte[] b, int off, int len) throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    stream.readFully(b, off, len);
  }

  /* @see java.io.DataInput.readInt() */
  public int readInt() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return adaptOrder(stream.readInt());
  }

  /* @see java.io.DataInput.readLine() */
  public String readLine() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return stream.readLine();
  }

  /* @see java.io.DataInput.readLong() */
  public long readLong() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return adaptOrder(stream.readLong());
  }

  /* @see java.io.DataInput.readShort() */
  public short readShort() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return adaptOrder(stream.readShort());
  }

  /* @see java.io.DataInput.readUnsignedByte() */
  public int readUnsignedByte() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return stream.readUnsignedByte();
  }

  /* @see java.io.DataInput.readUnsignedShort() */
  public int readUnsignedShort() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return readShort() & 0xffff;
  }

  /* @see java.io.DataInput.readUTF() */
  public String readUTF() throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return stream.readUTF();
  }

  /* @see java.io.DataInput.skipBytes(int) */
  public int skipBytes(int n) throws IOException {
    if (stream == null) {
      throw new HandleException("This stream is write-only.");
    }
    return (int) Math.min(stream.skipBytes(n), length());
  }

  // -- DataOutput API metthods --

  /* @see java.io.DataOutput.write(byte[]) */
  public void write(byte[] b) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.write(b);
  }

  /* @see java.io.DataOutput.write(byte[], int, int) */
  public void write(byte[] b, int off, int len) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.write(b, off, len);
  }

  /* @see java.io.DataOutput.write(int b) */
  public void write(int b) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.write(adaptOrder(b));
  }

  /* @see java.io.DataOutput.writeBoolean(boolean) */
  public void writeBoolean(boolean v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeBoolean(v);
  }

  /* @see java.io.DataOutput.writeByte(int) */
  public void writeByte(int v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeByte(adaptOrder(v));
  }

  /* @see java.io.DataOutput.writeBytes(String) */
  public void writeBytes(String s) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeBytes(s);
  }

  /* @see java.io.DataOutput.writeChar(int) */
  public void writeChar(int v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeChar(adaptOrder(v));
  }

  /* @see java.io.DataOutput.writeChars(String) */
  public void writeChars(String s) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeChars(s);
  }

  /* @see java.io.DataOutput.writeDouble(double) */
  public void writeDouble(double v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeDouble(adaptOrder(v));
  }

  /* @see java.io.DataOutput.writeFloat(float) */
  public void writeFloat(float v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeFloat(adaptOrder(v));
  }

  /* @see java.io.DataOutput.writeInt(int) */
  public void writeInt(int v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeInt(adaptOrder(v));
  }

  /* @see java.io.DataOutput.writeLong(long) */
  public void writeLong(long v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeLong(adaptOrder(v));
  }

  /* @see java.io.DataOutput.writeShort(int) */
  public void writeShort(int v) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeShort(adaptOrder(v));
  }

  /* @see java.io.DataOutput.writeUTF(String)  */
  public void writeUTF(String str) throws IOException {
    if (outStream == null) {
      throw new HandleException("This stream is read-only.");
    }
    outStream.writeUTF(str);
  }

  public ByteOrder getOrder() {
    return order;
  }

  public void setOrder(ByteOrder order) {
    this.order = order;
  }

}
