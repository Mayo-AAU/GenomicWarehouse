# HadoopCommons

HapoodCommons is a collection of Java and Scala utilities for connecting projects to a Hadoop stack.  It is intended to be useful for local development as well as remote deployment.

## Building

HadoopCommons uses [Maven](http://gradle.org/) as a build system.  [Maven](https://maven.apache.org/index.html) is an old standby build system for Java.  It may be [downloaded here](https://maven.apache.org/download.cgi), or installed via [MacPorts](https://www.macports.org/), e.g. `sudo port install maven`.

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

# Development

HadoopCommons uses [Hadoop mini-clusters](http://www.lopakalogic.com/articles/hadoop-articles/hadoop-testing-with-minicluster/) for local testing / debugging (see [https://github.com/sakserv/hadoop-mini-clusters](https://github.com/sakserv/hadoop-mini-clusters) for details).  At runtime, the `AutoConfigure` class provides a static method `getConfiguration()` to return a Hadoop configuration.  This configuration is either:

1. a global Hadoop configuration (found in `/etc/hbase/conf/hbase-site.xml`), or
2. a local Hadoop mini-cluster configuration to connect to the mini-cluster started by `AutoConfigure` and `MiniClusterUtil.startHBASE()`