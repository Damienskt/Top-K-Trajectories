
name := "top-k-trajectories"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.datasyslab" % "geospark" % "1.2.0"

resolvers += "bintray/meetup" at "http://dl.bintray.com/meetup/maven"
libraryDependencies += "com.meetup" %% "archery" % "0.4.0"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.0-M4")