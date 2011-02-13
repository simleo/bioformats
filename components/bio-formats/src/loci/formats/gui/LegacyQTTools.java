//
// LegacyQTTools.java
//

/*
OME Bio-Formats package for reading and converting biological file formats.
Copyright (C) 2005-@year@ UW-Madison LOCI and Glencoe Software, Inc.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package loci.formats.gui;

import java.awt.Dimension;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.image.DirectColorModel;
import java.awt.image.MemoryImageSource;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.StringTokenizer;

import loci.common.Location;
import loci.common.ReflectException;
import loci.common.ReflectedUniverse;
import loci.formats.FormatException;
import loci.formats.MissingLibraryException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for working with QuickTime for Java.
 *
 * <dl><dt><b>Source code:</b></dt>
 * <dd><a href="http://trac.openmicroscopy.org.uk/ome/browser/bioformats.git/components/bio-formats/src/loci/formats/gui/LegacyQTTools.java">Trac</a>,
 * <a href="http://git.openmicroscopy.org/?p=bioformats.git;a=blob;f=components/bio-formats/src/loci/formats/gui/LegacyQTTools.java;hb=HEAD">Gitweb</a></dd></dl>
 */
public class LegacyQTTools {

  // -- Constants --

  private static final Logger LOGGER =
    LoggerFactory.getLogger(LegacyQTTools.class);

  public static final String NO_QT_MSG =
    "QuickTime for Java is required to read some QuickTime files. " +
    "Please install QuickTime for Java from http://www.apple.com/quicktime/";

  public static final String JVM_64BIT_MSG =
    "QuickTime for Java is not supported with a 64-bit JVM. " +
    "Please invoke the 32-bit JVM (-d32) to utilize QTJava functionality.";

  public static final String EXPIRED_QT_MSG =
    "Your version of QuickTime for Java has expired. " +
    "Please reinstall QuickTime for Java from http://www.apple.com/quicktime/";

  protected static final String[] SUFFIXES = {"mov", "qt"};

  protected static final boolean MAC_OS_X =
    System.getProperty("os.name").equals("Mac OS X");

  // -- Static fields --

  /**
   * This custom class loader searches additional paths for the QTJava.zip
   * library. Java has a restriction where only one class loader can have a
   * native library loaded within a JVM. So the class loader must be static,
   * shared by all QTForms, or else an UnsatisfiedLinkError is thrown when
   * attempting to initialize QTJava multiple times.
   */
  protected static final ClassLoader LOADER = constructLoader();

  protected static ClassLoader constructLoader() {
    // set up additional QuickTime for Java paths
    URL[] paths = null;

    if (MAC_OS_X) {
      try {
        paths = new URL[] {
          new URL("file:/System/Library/Java/Extensions/QTJava.zip")
        };
      }
      catch (MalformedURLException exc) { LOGGER.info("", exc); }
      return paths == null ? null : new URLClassLoader(paths);
    }

    // case for Windows
    String windir = System.getProperty("java.library.path");
    StringTokenizer st = new StringTokenizer(windir, ";");

    while (st.hasMoreTokens()) {
      Location f = new Location(st.nextToken(), "QTJava.zip");
      if (f.exists()) {
        try {
          paths = new URL[] {f.toURL()};
        }
        catch (MalformedURLException exc) { LOGGER.info("", exc); }
        return paths == null ? null : new URLClassLoader(paths);
      }
    }

    return null;
  }

  // -- Fields --

  /** Flag indicating this class has been initialized. */
  protected boolean initialized = false;

  /** Flag indicating QuickTime for Java is not installed. */
  protected boolean noQT = false;

  /** Flag indicating 64-bit JVM (does not support QTJava). */
  protected boolean jvm64Bit = false;

  /** Flag indicating QuickTime for Java has expired. */
  protected boolean expiredQT = false;

  /** Reflection tool for QuickTime for Java calls. */
  protected ReflectedUniverse r;

  // -- LegacyQTTools API methods --

  /** Initializes the class. */
  protected void initClass() {
    if (initialized) return;

    String arch = System.getProperty("os.arch");
    if (arch != null && arch.indexOf("64") >= 0) {
      // QTJava is not supported on 64-bit Java; don't even try
      noQT = true;
      jvm64Bit = true;
      initialized = true;
      return;
    }

    boolean needClose = false;
    r = new ReflectedUniverse(LOADER);
    try {
      r.exec("import quicktime.QTSession");
      r.exec("QTSession.open()");
      needClose = true;

      // for LegacyQTReader and LegacyQTWriter
      r.exec("import quicktime.io.QTFile");
      r.exec("import quicktime.std.movies.Movie");

      // for LegacyQTReader
      r.exec("import quicktime.app.view.MoviePlayer");
      r.exec("import quicktime.app.view.QTImageProducer");
      r.exec("import quicktime.io.OpenMovieFile");
      r.exec("import quicktime.qd.QDDimension");
      r.exec("import quicktime.std.StdQTConstants");
      r.exec("import quicktime.std.movies.TimeInfo");
      r.exec("import quicktime.std.movies.Track");

      // for LegacyQTWriter
      r.exec("import quicktime.qd.QDGraphics");
      r.exec("import quicktime.qd.QDRect");
      r.exec("import quicktime.std.image.CSequence");
      r.exec("import quicktime.std.image.CodecComponent");
      r.exec("import quicktime.std.image.ImageDescription");
      r.exec("import quicktime.std.movies.media.VideoMedia");
      r.exec("import quicktime.util.QTHandle");
      r.exec("import quicktime.util.RawEncodedImage");
      r.exec("import quicktime.util.EndianOrder");
    }
    catch (ExceptionInInitializerError err) {
      noQT = true;
      Throwable t = err.getException();
      if (t instanceof SecurityException) {
        SecurityException exc = (SecurityException) t;
        if (exc.getMessage().indexOf("expired") >= 0) expiredQT = true;
      }
    }
    catch (Throwable t) {
      noQT = true;
      LOGGER.debug("Could not find QuickTime for Java", t);
    }
    finally {
      if (needClose) {
        try { r.exec("QTSession.close()"); }
        catch (Throwable t) {
          LOGGER.debug("Could not close QuickTime session", t);
        }
      }
      initialized = true;
    }
  }

  /** Whether QuickTime is available to this JVM. */
  public boolean canDoQT() {
    if (!initialized) initClass();
    return !noQT;
  }

  /** Whether this JVM is 64-bit. */
  public boolean isJVM64Bit() {
    if (!initialized) initClass();
    return jvm64Bit;
  }

  /** Whether QuickTime for Java has expired. */
  public boolean isQTExpired() {
    if (!initialized) initClass();
    return expiredQT;
  }

  /** Gets the QuickTime for Java version number. */
  public String getQTVersion() {
    if (isJVM64Bit()) return "Not available";
    else if (isQTExpired()) return "Expired";
    else if (!canDoQT()) return "Missing";
    else {
      try {
        String qtMajor = r.exec("QTSession.getMajorVersion()").toString();
        String qtMinor = r.exec("QTSession.getMinorVersion()").toString();
        return qtMajor + "." + qtMinor;
      }
      catch (Throwable t) {
        LOGGER.debug("Could not retrieve QuickTime for Java version", t);
        return "Error";
      }
    }
  }

  /** Gets QuickTime for Java reflected universe. */
  public ReflectedUniverse getUniverse() {
    if (!initialized) initClass();
    return r;
  }

  /** Gets width and height for the given PICT bytes. */
  public Dimension getPictDimensions(byte[] bytes)
    throws FormatException, ReflectException
  {
    checkQTLibrary();
    try {
      r.exec("QTSession.open()");
      r.setVar("bytes", bytes);
      r.exec("pict = new Pict(bytes)");
      r.exec("box = pict.getPictFrame()");
      int width = ((Integer) r.exec("box.getWidth()")).intValue();
      int height = ((Integer) r.exec("box.getHeight()")).intValue();
      r.exec("QTSession.close()");
      return new Dimension(width, height);
    }
    catch (ReflectException e) {
      r.exec("QTSession.close()");
      throw new FormatException("PICT height determination failed", e);
    }
  }

  /** Converts the given byte array in PICT format to a Java image. */
  public synchronized Image pictToImage(byte[] bytes)
    throws FormatException
  {
    checkQTLibrary();
    try {
      r.exec("QTSession.open()");

      // Code adapted from:
      //   http://www.onjava.com/pub/a/onjava/2002/12/23/jmf.html?page=2
      r.setVar("bytes", bytes);
      r.exec("pict = new Pict(bytes)");
      r.exec("box = pict.getPictFrame()");
      int width = ((Integer) r.exec("box.getWidth()")).intValue();
      int height = ((Integer) r.exec("box.getHeight()")).intValue();
      // note: could get a RawEncodedImage from the Pict, but
      // apparently no way to get a PixMap from the REI
      r.exec("g = new QDGraphics(box)");
      r.exec("pict.draw(g, box)");
      // get data from the QDGraphics
      r.exec("pixMap = g.getPixMap()");
      r.exec("rei = pixMap.getPixelData()");

      // copy bytes to an array
      int rowBytes = ((Integer) r.exec("pixMap.getRowBytes()")).intValue();
      int intsPerRow = rowBytes / 4;
      int pixLen = intsPerRow * height;
      r.setVar("pixLen", pixLen);
      int[] pixels = new int[pixLen];
      r.setVar("pixels", pixels);
      r.setVar("zero", new Integer(0));
      r.exec("rei.copyToArray(zero, pixels, zero, pixLen)");

      // now coax into image, ignoring alpha for speed
      int bitsPerSample = 32;
      int redMask = 0x00ff0000;
      int greenMask = 0x0000ff00;
      int blueMask = 0x000000ff;
      int alphaMask = 0x00000000;
      DirectColorModel colorModel = new DirectColorModel(
        bitsPerSample, redMask, greenMask, blueMask, alphaMask);

      r.exec("QTSession.close()");
      return Toolkit.getDefaultToolkit().createImage(new MemoryImageSource(
        width, height, colorModel, pixels, 0, intsPerRow));
    }
    catch (ReflectException e) {
      try { r.exec("QTSession.close()"); }
      catch (ReflectException exc) {
        LOGGER.info("Could not close QuickTime session", exc);
      }
      throw new FormatException("PICT extraction failed", e);
    }
  }

  /** Checks whether QTJava is available, throwing an exception if not. */
  public void checkQTLibrary() throws MissingLibraryException {
    if (isJVM64Bit()) throw new MissingLibraryException(JVM_64BIT_MSG);
    if (isQTExpired()) throw new MissingLibraryException(EXPIRED_QT_MSG);
    if (!canDoQT()) throw new MissingLibraryException(NO_QT_MSG);
  }

}
