


# Mighty Spark Clickhouse Plugin

[![spark-clickhouse-plugin](https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin.svg?style=svg)](https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin)
![https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin](https://img.shields.io/github/license/The-Analytics-Gladiators/spark-clickhouse-plugin)
![https://circleci.com/gh/The-Analytics-Gladiators/spark-clickhouse-plugin](https://img.shields.io/github/v/tag/The-Analytics-Gladiators/spark-clickhouse-plugin)

![gladiator](https://user-images.githubusercontent.com/739463/211081470-c122acee-781f-480e-b52e-48a8516529db.png)

Behold the most intuitive Spark Plugin for interacting with Clickhouse

## Usage

### Maven
```xml
  <profiles>
    <profile>
      <id>github</id>
      <repositories>
        <repository>
          <id>central</id>
          <url>https://repo1.maven.org/maven2</url>
        </repository>
        <repository>
          <id>github</id>
          <url>https://maven.pkg.github.com/The-Analytics-Gladiators/spark-clickhouse-plugin</url>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
    </profile>
  </profiles>
  <dependencies>
  <dependency>
        <groupId>io.gladiators</groupId>
        <artifactId>spark-clickhouse-plugin_2.12</artifactId>
        <version>0.16</version>
    </dependency> 
  </dependencies>

```

### SBT

```scala

resolvers += "Github Gladiator" at "https://maven.pkg.github.com/The-Analytics-Gladiators/spark-clickhouse-plugin"
libraryDependencies ++= Seq("io.gladiators" % "spark-clickhouse-plugin" % "0.16")

```


## Contributions

Contributions are welcome, but there are no guarantees that they are accepted as such. Process for contributing is the following:

* Fork this project
* Create an issue to this project about the contribution (bug or feature) if there is no such issue about it already. Try to keep the scope minimal.
* Develop and test the fix or functionality carefully. Only include minimum amount of code needed to fix the issue.
* Refer to the fixed issue in commit
* Send a pull request for the original project
* Comment on the original issue that you have implemented a fix for it


## License

Spark Clickhouse Plugin is licensed under the MIT license. See the LICENSE.txt file for more information.