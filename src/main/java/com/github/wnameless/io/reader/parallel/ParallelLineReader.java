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

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * 
 * {@link ParallelLineReader} reads lines by dividing the input into smaller
 * parts and then reads all parts parallelly by a thread {@link Executor}.
 * 
 * @author Wei-Ming Wu
 *
 */
public class ParallelLineReader {

  private final Supplier<? extends Reader> reader;
  private final int maxLines;

  private final Executor executor;

  /**
   * Creates a {@link ParallelLineReader} by given {@link Reader} and
   * {@link Executor}.
   * 
   * @param maxLines
   *          the max number of lines of each parts
   * @param reader
   *          a {@link Supplier} can provide {@link Reader}s of certain content
   * @param executor
   *          a Java {@link Executor}
   */
  public ParallelLineReader(int maxLines, Supplier<? extends Reader> reader,
      Executor executor) {
    this.reader = reader;
    this.maxLines = maxLines;
    this.executor = executor;
  }

  /**
   * Creates a {@link ParallelLineReader} by given {@link Reader}.
   * 
   * @param maxLines
   *          the max number of lines of each parts
   * @param reader
   *          a {@link Supplier} can provide {@link Reader}s of certain content
   */
  public ParallelLineReader(int maxLines, Supplier<? extends Reader> reader) {
    this.reader = reader;
    this.maxLines = maxLines;
    executor = null;
  }

  /**
   * Creates {@link CompletableFuture}s for each part of input content.
   * 
   * @param <E>
   *          the result type of each {@link CompletableFuture}
   * @param lineReaderFunction
   *          a function to process each part of the content which is read by a
   *          {@link LineReader}
   * @return a list of {@link CompletableFuture}s
   * @throws IOException
   *           if any I/O Exception happened during reading
   */
  public <E> List<CompletableFuture<E>> readParallelly(
      LineReaderFunction<E> lineReaderFunction) throws IOException {
    if (executor == null) {
      return LineReaders.readParallelly(reader, maxLines,
          lineReaderFunction);
    } else {
      return LineReaders.readParallelly(reader, maxLines,
          lineReaderFunction, executor);
    }
  }

}
