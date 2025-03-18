name := "COMP30770Maven"
version := "1.0-SNAPSHOT"
organization := "org.example"
scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.6",
  "org.apache.spark" % "spark-core_2.12" % "3.3.1",
  // Test dependencies
  "junit" % "junit" % "4.12" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.specs2" %% "specs2-core" % "4.2.0" % Test,
  "org.specs2" %% "specs2-junit" % "4.2.0" % Test
)

// Compile settings
scalacOptions := Seq(
  "-target:jvm-1.8", // Equivalent to maven.compiler.target
  "-sourcepath", "src/main/scala", // Equivalent to source directory
  "-deprecation" // Optional: To show deprecation warnings
)

// Test settings
testOptions in Test += Tests.Argument("-oD", "-s") // Equivalent to 'skipTests' in Maven
testFrameworks += new TestFramework("org.scalatest.tools.Framework")

// Additional sbt settings
compile in Compile := (compile in Compile).dependsOn(compile in Test).value
