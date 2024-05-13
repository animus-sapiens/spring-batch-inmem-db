# Spring Batch with  HSQLDB

## Table of contents
- [Project Overview](#project-overview)
- [Software Requirements](#software-requirements)
- [Building and execution](#building-and-execution)
- [License](#license)
- [Used Frameworks](#used-frameworks)


## Project Overview
Spring Boot Batch application configuration with using in test HSQLDB (HyperSQL DataBase) open-source RDBMS. 
HSQLDB is used as running in-memory to test Spring Batch job execution.

### Software requirements
- Java JDK 17+
- Gradle 7.4+
- Windows, Linux and macOS specified in
  <a target="_blank" href= https://www.oracle.com/java/technologies/javase/products-doc-jdk17certconfig.html >Oracle JDK 17 Certified System Configurations</a>
  or later.

### Building and execution
Build with test execution:
```
./gradlew build
```
Only test:
```
./gradlew test
```
For detailed test-log:
```
./gradlew test --debug
```

[!IMPORTANT]
<span style="font-size:30px;">&#9888;</span> In case test fail, pls increase WAIT_TIME_MILS.

### License

<p style="text-align: left;">
<a target="_blank" href=https://www.mozilla.org/en-US/MPL/2.0/>Mozilla Public License
Version 2.0</a>
</p>

### Used Frameworks
- https://spring.io/projects/spring-boot
- https://spring.io/projects/spring-batch
- https://hsqldb.org/
- https://junit.org/junit5/docs/current/user-guide/
- https://github.com/mockito/mockito
- https://gradle.org/
