import com.sun.jna.Platform

val compileNative = taskKey[Unit]("Compile cpp into shared library.")

name := "ann4s"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6")

libraryDependencies ++= Seq(
  "org.rocksdb" % "rocksdbjni" % "4.11.2",
  "net.java.dev.jna" % "jna" % "4.2.2",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

organization := "com.github.mskimm"

licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")

// adopts https://github.com/pishen/annoy4s
compileNative := {
  val libDir = file(s"src/main/resources/${Platform.RESOURCE_PREFIX}")
  if (!libDir.exists) {
    libDir.mkdirs()
  }
  val lib = if (Platform.RESOURCE_PREFIX == "darwin") {
    libDir / "libannoy.dylib"
  } else {
    libDir / "libannoy.so"
  }
  val source = file("src/main/cpp/annoyjava.cpp")
  val cmd = s"g++ -Iannoy/src -O3 -march=native -ffast-math -o ${lib.getAbsolutePath} -shared -fPIC ${source.getAbsolutePath}"
  println(cmd)
  import scala.sys.process._
  cmd.!
}

