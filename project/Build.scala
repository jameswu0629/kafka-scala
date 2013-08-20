import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "kafka-test"
    val appVersion      = "1.0-SNAPSHOT"

    val appDependencies = Seq(
      // Add your project dependencies here,
      "com.yammer.metrics" % "metrics-core" % "2.2.0",
      "org.apache.kafka" % "kafka_2.9.1" % "0.8.0-beta1",
      "log4j" % "log4j" % "1.2.15" excludeAll(
        ExclusionRule(organization = "com.sun.jdmk"),
        ExclusionRule(organization = "com.sun.jmx"),
        ExclusionRule(organization = "javax.jms")
        )
    )

    //val main = PlayProject(appName, appVersion, appDependencies, mainLang = SCALA).settings(
    val main = PlayProject(appName, appVersion, appDependencies).settings(defaultScalaSettings:_*).settings(
        
      // Add your own project settings here      
      resolvers += (
          "Local Maven Repository" at "file:///Users/jameswu/.m2/repository"
      )
    )

}
