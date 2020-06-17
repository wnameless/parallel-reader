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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import net.sf.rubycollect4j.util.WholeLineReader;

/**
 * 
 * {@link LineReaders} is an utility class. It provides all kinds of static
 * methods to read lines parallelly from an input content.
 * 
 * @author Wei-Ming Wu
 *
 */
public final class LineReaders {

  private LineReaders() {}

  /**
   * Reads lines of content parallelly by dividing the input into smaller parts.
   * 
   * @param <E>
   *          the type of returning value
   * @param reader
   *          which contents lines
   * @param maxLines
   *          the max number of lines of each parts
   * @param lineReaderFunction
   *          a function to process each part of the content which is read by a
   *          {@link LineReader}
   * @return a list of {@link CompletableFuture}s
   * @throws IOException
   *           if any I/O Exception happened during reading
   */
  public static <E> List<CompletableFuture<E>> readParallelly(
      Supplier<? extends Reader> reader, int maxLines,
      LineReaderFunction<E> lineReaderFunction) throws IOException {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getSkipPoints(reader.get(), maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderFunction.apply(ii,
            toLineReader(reader.get(), skipPoints.get(ii), maxLines));
      });

      futures.add(future);
    }

    return futures;
  }

  /**
   * Reads lines of content parallelly by dividing the input into smaller parts.
   * 
   * @param <E>
   *          the type of returning value
   * @param reader
   *          which contents lines
   * @param maxLines
   *          the max number of lines of each parts
   * @param lineReaderFunction
   *          a function to process each part of the content which is read by a
   *          {@link LineReader}
   * @param executor
   *          a Java {@link Executor} to use
   * @return a list of {@link CompletableFuture}s
   * @throws IOException
   *           if any I/O Exception happened during reading
   */
  public static <E> List<CompletableFuture<E>> readParallelly(
      Supplier<? extends Reader> reader, int maxLines,
      LineReaderFunction<E> lineReaderFunction, Executor executor)
      throws IOException {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getSkipPoints(reader.get(), maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderFunction.apply(ii,
            toLineReader(reader.get(), skipPoints.get(ii), maxLines));
      }, executor);

      futures.add(future);
    }

    return futures;
  }

  /**
   * Reads lines of content parallelly by dividing the input into smaller parts.
   * 
   * @param <E>
   *          the type of returning value
   * @param file
   *          which contents lines
   * @param maxLines
   *          the max number of lines of each parts
   * @param lineReaderFunction
   *          a function to process each part of the content which is read by a
   *          {@link LineReader}
   * @return a list of {@link CompletableFuture}s
   * @throws IOException
   *           if any I/O Exception happened during reading
   */
  public static <E> List<CompletableFuture<E>> readParallelly(File file,
      int maxLines, LineReaderFunction<E> lineReaderFunction)
      throws IOException {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getPartitionPoints(file, maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderFunction.apply(ii,
            toLineReader(file, skipPoints.get(ii), maxLines));
      });

      futures.add(future);
    }

    return futures;
  }

  /**
   * Reads lines of content parallelly by dividing the input into smaller parts.
   * 
   * @param <E>
   *          the type of returning value
   * @param file
   *          which contents lines
   * @param maxLines
   *          the max number of lines of each parts
   * @param lineReaderFunction
   *          a function to process each part of the content which is read by a
   *          {@link LineReader}
   * @param executor
   *          a Java {@link Executor} to use
   * @return a list of {@link CompletableFuture}s
   * @throws IOException
   *           if any I/O Exception happened during reading
   */
  public static <E> List<CompletableFuture<E>> readParallelly(File file,
      int maxLines, LineReaderFunction<E> lineReaderFunction, Executor executor)
      throws IOException {
    List<CompletableFuture<E>> futures = new ArrayList<>();

    List<Long> skipPoints = getPartitionPoints(file, maxLines);

    for (int i = 0; i < skipPoints.size(); i++) {
      int ii = i;
      CompletableFuture<E> future = CompletableFuture.supplyAsync(() -> {
        return lineReaderFunction.apply(ii,
            toLineReader(file, skipPoints.get(ii), maxLines));
      }, executor);

      futures.add(future);
    }

    return futures;
  }

  /**
   * Creates a {@link LineReader} to read part of lines of a {@link Reader}.
   * 
   * @param reader
   *          which contents lines
   * @param skip
   *          characters to be skipped
   * @param maxLines
   *          the max number of lines of each parts
   * @return a {@link LineReader}
   */
  public static LineReader toLineReader(Reader reader, long skip,
      int maxLines) {
    return new LineReader(reader, skip, maxLines);
  }

  /**
   * Creates a {@link LineReader} to read part of lines of a {@link File}.
   * 
   * @param file
   *          which contents lines
   * @param position
   *          bytes to be skipped
   * @param maxLines
   *          the max number of lines of each parts
   * @return a {@link LineReader}
   */
  public static LineReader toLineReader(File file, long position,
      int maxLines) {
    return new LineReader(file, position, maxLines);
  }

  /**
   * Finds out all partition points of a {@link Reader} based on the given max
   * line number. Each skip point represents lengths of characters starting from
   * the beginning of a {@link Reader}. Any 2 successive points means all
   * characters within this interval contain a max number of lines or less if it
   * is the last part.
   * 
   * @param file
   *          which contains lines
   * @param maxLines
   *          the max number of lines of each parts
   * @return a list of Long numbers represents lengths of characters start from
   *         0
   * @throws IOException
   *           if any I/O Exception happened during parsing
   */
  public static List<Long> getSkipPoints(Reader reader, int maxLines)
      throws IOException {
    WholeLineReader wlr = new WholeLineReader(reader);

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

  /**
   * Finds out all partition points of a file based on the given max line
   * number. Each partition point represents lengths of bytes starting from the
   * beginning of a file. Any 2 successive points means all bytes within this
   * interval contain a max number of lines or less if it is the last part.
   * 
   * @param file
   *          which contains lines
   * @param maxLines
   *          the max number of lines of each parts
   * @return a list of Long numbers represents lengths of bytes start from 0
   * @throws IOException
   *           if any I/O Exception happened during parsing
   */
  public static List<Long> getPartitionPoints(File file, int maxLines)
      throws IOException {
    WholeLineReader wlr = new WholeLineReader(new FileReader(file));

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
