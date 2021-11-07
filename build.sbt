
name := "spark-clickhouse-plugin"

version := "0.0.1"

scalaVersion := "2.12.15"

val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
)
