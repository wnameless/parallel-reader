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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import net.sf.rubycollect4j.Ruby;

public class ParallelLineReaderTest {

  static ParallelLineReader plr;

  @BeforeAll
  public static void setup() {
    plr = new ParallelLineReader(2, () -> {
      try {
        return new FileReader("src/test/resources/test.csv");
      } catch (FileNotFoundException e) {}
      return null;
    });
  }

  @Test
  public void testParallelRead()
      throws InterruptedException, ExecutionException {
    List<CompletableFuture<String>> futures = plr.parallelRead((part, lr) -> {
      String str = "";

      while (lr.hasNext()) {
        str += lr.readLineQuietly();
      }

      lr.closeQuietly();
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
