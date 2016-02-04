name := """bss"""

organization := "com.github.brainstorm"

version := "0.1"

scalaVersion := "2.11.7"


libraryDependencies ++= {
  val akkaVersion = "2.4.1"

  Seq(
    "org.slf4j"                     % "slf4j-api"       % "1.7.14",
    "org.slf4j"                     % "slf4j-simple"    % "1.7.14",
    "com.typesafe.akka"            %% "akka-actor"      % akkaVersion,
    "org.rocksdb"                   % "rocksdbjni"      % "4.1.0",
    "com.github.scopt"             %% "scopt"           % "3.3.0",

    "com.typesafe.akka"            %% "akka-testkit"    % akkaVersion    % "test",
    "org.scalatest"                %% "scalatest"       % "2.2.6"        % "test"
  )
}

fork in run := true

//mainClass in Compile := Some("bss.tools.Watch")
mainClass in Compile := Some("bss.tools.Watch")
//mainClass in Compile := Some("bss.tools.Snapshot")

scalariformSettings

enablePlugins(JavaAppPackaging)

enablePlugins(DockerPlugin)

dockerExposedVolumes in Docker := Seq(
  "/opt/docker"
)