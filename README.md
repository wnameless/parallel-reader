[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.wnameless.io/parallel-reader/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.wnameless.io/parallel-reader)

parallel-reader
=============
A Java Reader can read lines from a file or a reader parallelly

## Purpose
Reading a big file line by line can be quite time consumption. It is often due to the IO bottleneck rather than the CPU power. Dividing a big file into smaller parts and reading all of it parallelly can be a solution.

# Maven Repo
```xml
<dependency>
	<groupId>com.github.wnameless.io</groupId>
	<artifactId>parallel-reader</artifactId>
	<version>1.0.0</version>
</dependency>
```

## Quick Start

Reads a content parallelly.
<br>
LineReaders with Reader:
```java
// Reader must be provided by a Supplier
Supplier<Reader> readerSupplier = () -> new StringReader("1\n2\n3\n4\n5\n");
// the max line number of each part
int maxLines = 2;

List<CompletableFuture<Integer>> futures =
    LineReaders.readParallelly(readerSupplier, maxLines,
        (part /* the index of each part starting from 0 */, lineReader) -> {
          	while (lineReader.hasNext()) {
            		String line = lineReader.readLineQuietly();
            		System.out.println(line);
            }

            return part;
        });
```

LineReaders with File:
```java
File file =new File("path_to_your_file");
// the max line number of each part
int maxLines = 2;

List<CompletableFuture<Integer>> futures =
	LineReaders.readParallelly(file , maxLines,
		(part /* the index of each part starting from 0 */, lineReader) -> {
        		while (lineReader.hasNext()) {
            		String line = lineReader.readLineQuietly();
            		System.out.println(line);
        		}

        		return part;
    		});
```

LineReaders with Executor:
```java
// Optional
Executor executor = Executors.newFixedThreadPool(4);

File file =new File("path_to_your_file");
// the max line number of each part
int maxLines = 2;

List<CompletableFuture<Integer>> futures =
	LineReaders.readParallelly(file , maxLines,
		(part /* the index of each part starting from 0 */, lineReader) -> {
        		while (lineReader.hasNext()) {
            		String line = lineReader.readLineQuietly();
            		System.out.println(line);
        		}

        		return part;
    		}, executor);
```
