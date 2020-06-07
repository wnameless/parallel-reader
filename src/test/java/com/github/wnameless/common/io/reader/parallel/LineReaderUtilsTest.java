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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import net.sf.rubycollect4j.Ruby;

public class LineReaderUtilsTest {

  @Test
  public void testgGetSkipPoints() throws FileNotFoundException {
    List<Long> skipPoints = LineReaderUtils
        .getSkipPoints(new FileReader("src/test/resources/test.csv"), 1);

    assertEquals(Arrays.asList(
        new Long[] { 0L, 3L, 5L, 7L, 9L, 11L, 13L, 15L, 17L, 19L, 22L, 26L }),
        skipPoints);
  }

  @Test
  public void testParallelLineReader()
      throws InterruptedException, ExecutionException {
    Supplier<Reader> reader = () -> {
      try {
        return new FileReader("src/test/resources/test.csv");
      } catch (FileNotFoundException e) {}
      return null;
    };

    List<CompletableFuture<String>> futures =
        LineReaderUtils.parallelLineReader(reader, 2, (part, lr) -> {
          String str = "";

          while (lr.hasNext()) {
            str += lr.readLineQuietly();
          }

          return str;
        });

    while (!Ruby.Array.of(futures).map(f -> f.isDone()).allʔ()) {}

    String res = "";
    for (CompletableFuture<String> cf : futures) {
      res += cf.get();
    }

    assertEquals("1234567891011", res);
  }

  @Test
  public void testParallelLineReaderWithFile()
      throws InterruptedException, ExecutionException {
    List<CompletableFuture<String>> futures =
        LineReaderUtils.parallelLineReader(
            new File("src/test/resources/test.csv"), 2, (part, lr) -> {
              String str = "";

              while (lr.hasNext()) {
                str += lr.readLineQuietly();
              }

              return str;
            });

    while (!Ruby.Array.of(futures).map(f -> f.isDone()).allʔ()) {}

    String res = "";
    for (CompletableFuture<String> cf : futures) {
      res += cf.get();
    }

    assertEquals("1234567891011", res);
  }

}
