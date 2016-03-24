# Development


# Hadoop


[Hadoop mini-clusters](http://www.lopakalogic.com/articles/hadoop-articles/hadoop-testing-with-minicluster/) - [https://github.com/sakserv/hadoop-mini-clusters](https://github.com/sakserv/hadoop-mini-clusters)


Docker-Ambari server (can be done on Mac), can be used to deploy a local cluster to test deployment.  Seems to be most useful for Ambari development, less so for the components.


`spark-submit` can be used locally.  It can also be used to connect to a cluster.  Used in local mode, and called from Scala or Java code, breakpoints can be inserted into developers code.

# Gradle

[Gradle](http://gradle.org/) is a modern open source polyglot build automation system.  Gradle is far more concise than [Maven](https://maven.apache.org/) providing a full DevOps environment to the developer.  That's the claim anyway, we'll see.

To build the `bd4g` code:

``` bash
./gradlew build
```

To prepare for Eclipse development

``` bash
./gradlew eclipse
```

To prepare for IntelliJ development

``` 
./gradlew idea
```

## Dependancies

To add a dependancy, edit the `build.gradle` file, looking for the `dependancies` section.

``` groovy
dependencies {
  compile (
    'org.apache.hbase:hbase-client:1.1.2',
```

Should be mostly self-explanatory.

