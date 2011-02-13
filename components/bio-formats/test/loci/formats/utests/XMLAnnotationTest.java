//
// XMLAnnotationTest.java
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

package loci.formats.utests;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import ome.xml.model.Annotation;
import ome.xml.model.Channel;
import ome.xml.model.Image;
import ome.xml.model.OME;
import ome.xml.model.OMEModel;
import ome.xml.model.OMEModelImpl;
import ome.xml.model.Pixels;
import ome.xml.model.XMLAnnotation;
import ome.xml.model.enums.EnumerationException;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.w3c.dom.Document;

/**
 * 
 * Test case which outlines the problems seen in omero:#3269.
 * 
 * @author Chris Allan <callan at blackcat dot ca>
 *
 *
 * <dl><dt><b>Source code:</b></dt>
 * <dd><a href="http://trac.openmicroscopy.org.uk/ome/browser/bioformats.git/components/bio-formats/test/loci/formats/utests/XMLAnnotationTest.java">Trac</a>,
 * <a href="http://git.openmicroscopy.org/?p=bioformats.git;a=blob;f=components/bio-formats/test/loci/formats/utests/XMLAnnotationTest.java;hb=HEAD">Gitweb</a></dd></dl>
 */
public class XMLAnnotationTest {

  private OME ome;

  @BeforeClass
  public void setUp() throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder parser = factory.newDocumentBuilder();
    Document document = parser.parse(
        this.getClass().getResourceAsStream("XMLAnnotationTest.ome"));
    OMEModel model = new OMEModelImpl();
    // Read string XML in as a DOM tree and parse into the object hierarchy
    ome = new OME(document.getDocumentElement(), model);
    model.resolveReferences();
  }

  @Test
  public void testValidXMLAnnotation() throws EnumerationException {
    assertNotNull(ome);
    assertEquals(1, ome.sizeOfImageList());
    Image image = ome.getImage(0);
    Pixels pixels = image.getPixels();
    assertNotNull(pixels);
    assertEquals(3, pixels.sizeOfChannelList());
    Channel channel = pixels.getChannel(0);
    assertEquals(1, channel.sizeOfLinkedAnnotationList());
    Annotation annotation = channel.getLinkedAnnotation(0);
    assertEquals(XMLAnnotation.class, annotation.getClass());
    assertEquals("<TestData><key>foo</key><value>bar</value></TestData>",
                 ((XMLAnnotation) annotation).getValue());
  }

}
