ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "untitled3",

    libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.26",
    libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.10",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.2",
    libraryDependencies += "net.liftweb" %% "lift-json" % "3.5.0",
    libraryDependencies += "com.opencsv" % "opencsv" % "5.5.2",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2",
      "org.apache.spark" %% "spark-streaming" % "3.3.2",
      "org.apache.spark" %% "spark-hive" % "3.3.2"

    )
  )
