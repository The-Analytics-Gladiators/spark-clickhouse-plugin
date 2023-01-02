import sbtrelease._
import sbtrelease.ReleaseStateTransformations._

resolvers += Resolver.mavenLocal

name := "spark-clickhouse-plugin"

scalaVersion := "2.12.15"

val sparkVersion = "3.2.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.clickhouse" % "clickhouse-jdbc" % "0.3.2-patch11",
  "com.github.bigwheel" %% "util-backports" % "2.1", //backport of scala utils for 2.12

  "com.holdenkarau" %% "spark-testing-base" % "3.2.0_1.1.1" % "it,test",
  "org.typelevel" %% "discipline-scalatest" % "2.1.5" % "it,test"
)

configs(IntegrationTest)
Defaults.itSettings
parallelExecution in IntegrationTest := false

assemblyMergeStrategy in assembly := {
  case PathList("com", "clickhouse", xs @ _*) => MergeStrategy.first
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs@_*) =>
    (xs map {
      _.toLowerCase
    }) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

releaseIgnoreUntrackedFiles := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)

publishMavenStyle := true

publishTo := Some(
  "GitHub Package Registry " at "https://maven.pkg.github.com/The-Analytics-Gladiators/spark-clickhouse-plugin"
)

(sys.env.get("GITHUB_USERNAME"), sys.env.get("GITHUB_TOKEN")) match {
  case (Some(username), Some(token)) =>
    credentials += Credentials(
      "GitHub Package Registry",
      "maven.pkg.github.com",
      username,
      token
    )
  case _ =>
    println("No github token found")
    credentials ++= Seq()
}
