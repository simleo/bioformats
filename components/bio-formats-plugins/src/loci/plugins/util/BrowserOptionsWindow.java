/*
 * #%L
 * Bio-Formats Plugins for ImageJ: a collection of ImageJ plugins including the
 * Bio-Formats Importer, Bio-Formats Exporter, Bio-Formats Macro Extensions,
 * Data Browser and Stack Slicer.
 * %%
 * Copyright (C) 2006 - 2014 Open Microscopy Environment:
 *   - Board of Regents of the University of Wisconsin-Madison
 *   - Glencoe Software, Inc.
 *   - University of Dundee
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the 
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public 
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

package loci.plugins.util;

import java.awt.Dimension;

import javax.swing.JFrame;
import javax.swing.border.EmptyBorder;

import loci.formats.cache.Cache;
import loci.formats.gui.CacheComponent;

/**
 * Extension of JFrame that allows the user to adjust caching settings.
 */
public class BrowserOptionsWindow extends JFrame {

  // -- Constructor --

  public BrowserOptionsWindow(String title, Cache cache, String[] axes) {
    super(title);

    CacheComponent panel = new CacheComponent(cache, axes);

    panel.setBorder(new EmptyBorder(15, 15, 15, 15));
    panel.setMinimumSize(new Dimension(300, 500));
    setContentPane(panel);
    setDefaultCloseOperation(DISPOSE_ON_CLOSE);
    pack();
  }

}
