ThisBuild / scalaVersion := "2.13.12"

ThisBuild / libraryDependencies += compilerPlugin("org.typelevel" %% "kind-projector"     % "0.13.2" cross CrossVersion.full)
ThisBuild / libraryDependencies += compilerPlugin("com.olegpy"    %% "better-monadic-for" % "0.3.1")

lazy val root = (project in file("."))
	.settings(
		name := "http4s-multipart",
		libraryDependencies ++= Seq(
			"org.log4s" %% "log4s" % "1.10.0",
			"ch.qos.logback" % "logback-classic" % "1.4.14",
			"org.http4s" %% "http4s-ember-server" % "0.23.25",
			"org.http4s" %% "http4s-core" % "0.23.25",
		),
		Compile / run / fork := true,
	)

