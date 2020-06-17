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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

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
    assertNull(lr.readLine());
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
    assertNull(lr.readLine());
    lr.close();

    assertEquals("12", res);
  }

}
