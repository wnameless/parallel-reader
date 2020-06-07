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
package com.github.wnameless.common.io.reader.parallel;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * 
 * {@link LineReader}
 * 
 * @author Wei-Ming Wu
 *
 */
public class LineReader implements Closeable {

  private final BufferedReader br;
  private final int maxLines;

  private int currentLine = 0;

  private String peek;

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

  public boolean hasNext() {
    return peek != null;
  }

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

  public void closeQuietly() {
    try {
      br.close();
    } catch (IOException e) {}
  }

}
