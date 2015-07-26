name := "EaglePeak"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.6"

resolvers ++= Seq(
  //  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
)


libraryDependencies ++= {
  val sparkVer = "1.4.0"
  val luceneVer = "5.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-mllib" % sparkVer,
    "com.databricks" %% "spark-csv" % "1.0.3",
    "net.ceedubs" %% "ficus" % "1.1.2",
    "net.codingwell" %% "scala-guice" % "4.0.0",
    "org.monifu" %% "monifu" % "1.0-M2",
    "com.twitter" %% "chill" % "0.5.0",
    "org.specs2" %% "specs2-core" % "3.6.1" % "test"
  )
}

scalacOptions in Test ++= Seq("-Yrangepos")
//testOptions in Test += Tests.Argument("sequential")
