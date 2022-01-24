
name := "spark-clickhouse-plugin"

version := "0.0.1"

scalaVersion := "2.12.15"

val sparkVersion = "3.2.0"
parallelExecution in Test := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.3.2",
  "com.github.bigwheel" %% "util-backports" % "2.1", //backport of scala utils for 2.12

  "com.holdenkarau" %% "spark-testing-base" % "3.2.0_1.1.1" % Test,
  "org.typelevel" %% "discipline-scalatest" % "2.1.5" % Test

)
