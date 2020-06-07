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

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import net.sf.rubycollect4j.util.WholeLineReader;

@UtilityClass
public class LineReaderUtils {

  public <E> List<CompletableFuture<E>> parallelLineReader(
      Supplier<? extends Reader> reader, int maxLines,
      LineReaderFunction<E> lineReaderConsumer) {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getSkipPoints(reader.get(), maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderConsumer.apply(ii,
            toLineReader(reader.get(), skipPoints.get(ii), maxLines));
      });

      futures.add(future);
    }

    return futures;
  }

  public <E> List<CompletableFuture<E>> parallelLineReader(
      Supplier<? extends Reader> reader, int maxLines,
      LineReaderFunction<E> lineReaderConsumer, Executor executor) {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getSkipPoints(reader.get(), maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderConsumer.apply(ii,
            toLineReader(reader.get(), skipPoints.get(ii), maxLines));
      }, executor);

      futures.add(future);
    }

    return futures;
  }

  public <E> List<CompletableFuture<E>> parallelLineReader(File file,
      int maxLines, LineReaderFunction<E> lineReaderConsumer) {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getPartitionPoints(file, maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderConsumer.apply(ii,
            toLineReader(file, skipPoints.get(ii), maxLines));
      });

      futures.add(future);
    }

    return futures;
  }

  public <E> List<CompletableFuture<E>> parallelLineReader(File file,
      int maxLines, LineReaderFunction<E> lineReaderConsumer,
      Executor executor) {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getPartitionPoints(file, maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderConsumer.apply(ii,
            toLineReader(file, skipPoints.get(ii), maxLines));
      }, executor);

      futures.add(future);
    }

    return futures;
  }

  public LineReader toLineReader(Reader reader, long skip, int maxLines) {
    return new LineReader(reader, skip, maxLines);
  }

  public LineReader toLineReader(File file, long position, int maxLines) {
    return new LineReader(file, position, maxLines);
  }

  @SneakyThrows
  public List<Long> getSkipPoints(Reader reader, int maxLines) {
    WholeLineReader wlr = new WholeLineReader(reader);

    // BufferedReader bufferedReader = new BufferedReader(reader);

    List<Long> skipPoints = new ArrayList<>();
    skipPoints.add(0L);

    long skipPoint = 0;
    long lineNum = 0;

    String line = wlr.readLine();
    while (line != null) {
      lineNum++;
      skipPoint += line.length();

      if (lineNum % maxLines == 0) {
        skipPoints.add(skipPoint);
      }

      line = wlr.readLine();
    }

    wlr.close();
    return skipPoints;
  }

  @SneakyThrows
  public List<Long> getPartitionPoints(File file, int maxLines) {
    WholeLineReader wlr = new WholeLineReader(new FileReader(file));

    // BufferedReader bufferedReader = new BufferedReader(reader);

    List<Long> skipPoints = new ArrayList<>();
    skipPoints.add(0L);

    long skipPoint = 0;
    long lineNum = 0;

    String line = wlr.readLine();
    while (line != null) {
      lineNum++;
      skipPoint += line.getBytes().length;

      if (lineNum % maxLines == 0) {
        skipPoints.add(skipPoint);
      }

      line = wlr.readLine();
    }

    return skipPoints;
  }

}
