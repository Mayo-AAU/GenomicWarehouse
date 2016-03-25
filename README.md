# GenomicWarehouse [![Build Status](https://travis-ci.org/Mayo-AAU/GenomicWarehouse.svg?branch=master)](https://travis-ci.org/Mayo-AAU/GenomicWarehouse)
This is an open source project that attempts to answer important genomics questions on the big data platform


# High Level Goals

1. [Recalculate](doc/recalculate.md)
2. [Ingest](doc/ingest.md)
3. [Export](doc/export.md)
4. [Filter](doc/filter.md)
5. [Interaction](doc/interaction.md)

## Setup requirements

- Essentials
  - [X] Be able to scale cluster up and down as needed
  - [X] [Hortonworks Data Platform](http://hortonworks.com/hdp/) `v2.3.4` installed on Azure
  - [x] Install Java v8
  - [x] HDFS
  - [x] Spark
  - [x] HBase
  - [ ] Create infrastructure for evaluation
	 1. [local sandbox](doc/setup.md) for testing / development
	 2. "small cluster", 3x 8C/32G/10T+
	 3. "medium cluster", 5x 8C/32G/10T+
	 4. "large cluster", 7x 8C/32G/10T+
- Optional, but nice to have
  - [ ] Jypter
  - [ ] Zepplin

## Plan

- [X] Write [ingest code](doc/ingest.md)
   1. create schema in HBase
   2. ingest sample `.gvcf` files
- [ ] Write [filter code](doc/filter.md)
   1. identity filter
- [ ] Load sample datasets
- [ ] Write [export code](doc/export.md)
- [ ] Write [recalculate code](doc/recalculate.md)
- [ ] Write final report in *Google Documents*



# Building

This project uses [Maven](http://maven.apache.org/) as a build system.  [Maven](https://maven.apache.org/index.html) is an old standby build system for Java.  It may be [downloaded here](https://maven.apache.org/download.cgi), or installed via [MacPorts](https://www.macports.org/), e.g. `sudo port install maven`.  GenomicWarehouse is build on Java 1.8 or higher.

For Maven <del>noobs</del> beginners, there is a nice [getting started guide](http://maven.apache.org/guides/getting-started/index.htm).

To build the `hadoop-commons` code:

``` bash
mvn compile
```

To run the set of unit tests:

```bash
mvn test
```

To include the (longer) integration tests:

```bash
mvn failsafe:integration-test
```

Getting Maven to [fetch source and javadoc](http://tedwise.com/2010/01/27/maven-micro-tip-get-sources-and-javadocs/) is easy:

```bash
mvn dependency:sources
mvn dependency:resolve -Dclassifier=javadoc
```

### Dependancies

To add a dependency, edit the `pom.xml` file, looking for the `<dependancies>` section, *e.g.*

```xml
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.10</artifactId>
            <version>1.5.2</version>
        </dependency>
```

Should be mostly self-explanatory.

## Deployment

To deploy the CLI, build a jar file with an embedded classpath.  This is done through this [Maven plugin](http://stackoverflow.com/questions/23013941/how-to-put-all-dependencies-in-separate-folder-for-runnable-jar):

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-jar-plugin</artifactId>
    <configuration>
    <archive>
        <manifest>
        <addClasspath>true</addClasspath>
        <classpathPrefix>lib/</classpathPrefix>
        <mainClass>edu.mayo.genomics.LoadVCF</mainClass>
        </manifest>
    </archive>
    </configuration>
</plugin>
```

Maven can also collect the library jar files into `target/lib`:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-dependency-plugin</artifactId>
    <version>2.5.1</version>
    <executions>
      <execution>
        <id>copy-dependencies</id>
        <phase>package</phase>
        <goals>
        <goal>copy-dependencies</goal>
        </goals>
        <configuration>
        <outputDirectory>${project.build.directory}/lib/</outputDirectory>
        </configuration>
      </execution>
    </executions>
</plugin>            
```

Install on cluster:

```bash
rsync -r --progress target/warehouse-1.0-SNAPSHOT.jar target/lib cluster:path/
```




