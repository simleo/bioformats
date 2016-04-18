/*
 * #%L
 * BSD implementations of Bio-Formats readers and writers
 * %%
 * Copyright (C) 2005 - 2016 Open Microscopy Environment:
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

package loci.formats;

import java.math.BigInteger;

/**
 * FilePatternBlock represents a single block in a {@link loci.formats.FilePattern}.
 *
 * Examples:
 * <ul>
 *   <li>&lt;1-12&gt;</li>
 *   <li>&lt;A-H&gt;</li>
 *   <li>&lt;R,G,B&gt;</li>
 * </ul>
 *
 */
public class FilePatternBlock {

  // -- Constants --

  public static final String BLOCK_START = "<";
  public static final String BLOCK_END = ">";

  // -- Fields --

  /** Elements within this block, e.g. ["R", "G", "B"] or ["1", "2", "3"]. */
  private String[] elements;

  /** Whether or not this is a fixed-width block. */
  private boolean fixed;

  /** Whether or not this is a numeric block. */
  private boolean numeric;

  /** The number of leading zeroes. */
  private int zeroes;

  /** String representation of this block. */
  private String block;

  private BigInteger begin = null, end = null, step = null;

  // -- Constructor --

  public FilePatternBlock(String block) {
    this.block = block;
    explode();
  }

  // -- FilePatternBlock API methods --

  public String[] getElements() {
    return elements;
  }

  public String getBlock() {
    return block;
  }

  public boolean isFixed() {
    return fixed;
  }

  public boolean isNumeric() {
    return numeric;
  }

  public BigInteger getFirst() {
    return begin;
  }

  public BigInteger getLast() {
    return end;
  }

  public BigInteger getStep() {
    return step;
  }

  // -- Helper methods --

  private void throwBadBlock(String msgTemplate) {
    throw new IllegalBlockException(String.format(msgTemplate, block));
  }

  private void throwBadBlock(String msgTemplate, Throwable cause) {
    throw new IllegalBlockException(
        String.format(msgTemplate, block), cause
    );
  }

  private void setNumeric() {
    numeric = true;
    for (String s: elements) {
      try {
        new BigInteger(s);
      } catch (NumberFormatException e) {
        numeric = false;
        break;
      }
    }
  }

  private void setFixed() {
    fixed = true;
    int L = elements[0].length();
    for (int i = 1; i < elements.length; i++) {
      if (elements[i].length() != L) {
        fixed = false;
        break;
      }
    }
  }

  private void explode() {
    if (!block.startsWith(BLOCK_START) || !block.endsWith(BLOCK_END)) {
      throwBadBlock("\"%s\": missing block delimiter(s)");
    }
    String trimmed = block.substring(
        BLOCK_START.length(), block.length() - BLOCK_END.length()
    );
    elements = trimmed.split(",", -1);
    if (elements.length > 1) {
      setNumeric();
      setFixed();
      return;
    }
    elements = elements[0].split("-");
    if (elements.length < 2) {
      setNumeric();
      fixed = true;
      return;
    }
    String b = elements[0];
    elements = elements[1].split(":", -1);
    String e = elements[0];
    String s = (elements.length < 2) ? "1" : elements[1];

    numeric = true;

    try {
      begin = new BigInteger(b);
      end = new BigInteger(e);
      step = new BigInteger(s);
    } catch (NumberFormatException badN) {
      numeric = false;
      try {
        begin = new BigInteger(b, Character.MAX_RADIX);
        end = new BigInteger(e, Character.MAX_RADIX);
        step = new BigInteger(s, Character.MAX_RADIX);
      } catch (NumberFormatException badL) {
        throwBadBlock("invalid range delimiter(s)", badL);
      }
    }

    fixed = b.length() == e.length();
    zeroes = 0;
    for (zeroes=0; zeroes<e.length(); zeroes++) {
      if (e.charAt(zeroes) != '0') break;
    }

    int count = end.subtract(begin).divide(step).intValue() + 1;
    elements = new String[count];

    for (int i=0; i<count; i++) {
      BigInteger v = begin.add(step.multiply(BigInteger.valueOf(i)));
      String value = numeric ? v.toString() : v.toString(Character.MAX_RADIX);
      if (!numeric) {
        if (Character.isLowerCase(b.charAt(0))) value = value.toLowerCase();
        else value = value.toUpperCase();
      }
      int padChars = fixed ? e.length() - value.length() : 0;
      elements[i] = value;
      for (int j=0; j<padChars; j++) {
        elements[i] = "0" + elements[i];
      }
    }
  }

}
