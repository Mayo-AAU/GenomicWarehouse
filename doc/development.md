# Development

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

