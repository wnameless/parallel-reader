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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.junit.jupiter.api.Test;

public class LineReaderTest {

  @Test
  public void testReadLine() throws IOException {
    LineReader lr =
        new LineReader(new FileReader("src/test/resources/test.csv"), 0, 2);

    String res = "";
    while (lr.hasNext()) {
      res += lr.readLine();
    }
    lr.close();

    assertEquals("12", res);
  }

  @Test
  public void testReadLine2() throws IOException {
    LineReader lr =
        new LineReader(new File("src/test/resources/test.csv"), 0, 2);

    String res = "";
    while (lr.hasNext()) {
      res += lr.readLine();
    }
    lr.close();

    assertEquals("12", res);
  }

  @Test
  public void testPerformance() throws IOException {
    File f = new File(
        "/Users/wmw/eclipse-workspace/amazon-billing/CUR/607016221647.csv");

    long start = System.currentTimeMillis();
    List<Long> skipPoints =
        LineReaderUtils.getSkipPoints(new FileReader(f), 50000);

    System.out.println((System.currentTimeMillis() - start) / 1000);
    System.out.println(skipPoints.size());

    start = System.currentTimeMillis();
    BufferedReader br = new BufferedReader(new FileReader(f));
    br.skip(skipPoints.get(skipPoints.size() - 1));
    System.out.println((System.currentTimeMillis() - start) / 1000);
    br.close();

    start = System.currentTimeMillis();
    RandomAccessFile raf = new RandomAccessFile(f, "r");
    raf.seek(skipPoints.get(skipPoints.size() - 1));
    System.out.println((System.currentTimeMillis() - start) / 1000);
    raf.close();

    start = System.currentTimeMillis();
    FileInputStream fis = new FileInputStream(f);
    fis.getChannel().position(skipPoints.get(skipPoints.size() - 1));
    System.out.println((System.currentTimeMillis() - start) / 1000);
    fis.close();
  }

}
