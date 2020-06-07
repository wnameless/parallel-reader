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

import java.io.Reader;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class ParallelLineReader {

  private final Supplier<? extends Reader> reader;
  private final int maxLines;

  private final Executor executor;

  public ParallelLineReader(int maxLines, Supplier<? extends Reader> reader,
      Executor executor) {
    this.reader = reader;
    this.maxLines = maxLines;
    this.executor = executor;
  }

  public ParallelLineReader(int maxLines, Supplier<? extends Reader> reader) {
    this.reader = reader;
    this.maxLines = maxLines;
    executor = null;
  }

  public <E> List<CompletableFuture<E>> parallelRead(
      LineReaderFunction<E> lineReaderConsumer) {
    if (executor == null) {
      return LineReaderUtils.parallelLineReader(reader, maxLines,
          lineReaderConsumer);
    } else {
      return LineReaderUtils.parallelLineReader(reader, maxLines,
          lineReaderConsumer, executor);
    }
  }

}
