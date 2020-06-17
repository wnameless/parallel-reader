/*
 *
 * Copyright 2020 Wei-Ming Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.github.wnameless.io.reader.parallel;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * 
 * {@link LineReader} reads lines from any {@link Reader} or {@link File} with a
 * limitation which is described by given parameters.
 * 
 * @author Wei-Ming Wu
 *
 */
public class LineReader implements Closeable {

  private final BufferedReader br;
  private final int maxLines;

  private int currentLine = 0;
  private String peek;

  /**
   * Creates a {@link LineReader} by given {@link File}.
   * 
   * @param file
   *          which contains lines
   * @param position
   *          bytes to be skipped
   * @param maxLines
   *          the max number of lines to read
   */
  public LineReader(File file, long position, int maxLines) {
    FileInputStream fis;
    try {
      fis = new FileInputStream(file);
      fis.getChannel().position(position);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.maxLines = maxLines;
    br = new BufferedReader(new InputStreamReader(fis));
    try {
      peek = br.readLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a {@link LineReader} by given {@link Reader}.
   * 
   * @param reader
   *          which contains lines
   * @param skip
   *          characters to be skipped
   * @param maxLines
   *          the max number of lines to read
   */
  public LineReader(Reader reader, long skip, int maxLines) {
    br = new BufferedReader(reader);
    this.maxLines = maxLines;
    try {
      br.skip(skip);
      peek = br.readLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns true if there is a next line existed.
   * 
   * @return true if there is a next line to be read, false otherwise
   */
  public boolean hasNext() {
    return peek != null;
  }

  /**
   * Reads a line from either a {@link File} or a {@link Reader}.
   * 
   * @return a String contains the content of a line
   * @throws IOException
   *           if an I/O Exception happened during reading
   */
  public String readLine() throws IOException {
    String line = peek;

    if (line != null && currentLine < maxLines) {
      currentLine++;

      if (currentLine < maxLines) {
        peek = br.readLine();
      } else {
        peek = null;
      }

      return line;
    }

    return null;
  }

  /**
   * Reads a line and turns any exception into a {@link RuntimeException}.
   * 
   * @return a String contains the content of a line
   */
  public String readLineQuietly() {
    String line = null;
    try {
      line = readLine();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return line;
  }

  @Override
  public void close() throws IOException {
    br.close();
  }

  /**
   * Closes this reader and suppresses any exception.
   */
  public void closeQuietly() {
    try {
      br.close();
    } catch (IOException e) {}
  }

}
