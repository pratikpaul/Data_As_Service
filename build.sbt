organization  := "com.self.dashboard"

version       := "0.1"

scalaVersion  := "2.11.7"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

resolvers += "conjars" at "http://conjars.org/repo"

resolvers += "pentaho" at "http://repository.pentaho.org/artifactory/repo"

resolvers += "hortonworks" at "http://repo.hortonworks.com/content/repositories/releases"

resolvers += "springrepo" at "http://repo.spring.io/plugins-release/"

libraryDependencies ++= {
  val akkaV = "2.3.5"
  val sprayV = "1.3.2"
  Seq(
    "io.spray"            %%  "spray-servlet" % sprayV,
    "io.spray"            %%  "spray-routing" % sprayV,
	"io.spray"            %%  "spray-client" % sprayV,
    "io.spray"            %% "spray-can" % sprayV,
    "io.spray"            %%  "spray-json" % sprayV,
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.slick"  %% "slick"          % "3.1.0",
    "commons-dbcp"        % "commons-dbcp"   % "1.4",
    "mysql" % "mysql-connector-java" % "5.1.38"
  )
}

libraryDependencies += "org.hamcrest" % "hamcrest-core" % "1.3"

libraryDependencies ++= Seq(
   "org.json" % "json" % "20090211",
   //"com.hcl.eihDrive" % "ApplicationData_Manipulator" % "0.0.1-SNAPSHOT",
   "org.hibernate" % "hibernate-core" % "5.2.12.Final",
   "dom4j" % "dom4j" % "1.6.1",
   "commons-net" % "commons-net" % "3.6",
   "commons-lang" % "commons-lang" % "2.6",
   "commons-logging" % "commons-logging" % "1.2",
   "org.apache.hive" % "hive-jdbc" % "1.2.1000.2.5.3.0-37",
   "org.apache.logging.log4j" % "log4j-api" % "2.6.2",
   "org.apache.thrift" % "libthrift" % "0.9.3",
   "javax.servlet" % "servlet-api" % "2.5",
   "javax.mail" % "javax.mail-api" % "1.5.1",
   "org.json4s" %% "json4s-native" % "3.3.0",
   "org.json4s" %% "json4s-jackson" % "3.3.0",
   "org.apache.httpcomponents" % "httpclient" % "4.5.2",
   "org.apache.calcite" % "calcite-core" % "1.8.0",
   "com.facebook.presto" % "presto-jdbc" % "0.150",
   "org.apache.shiro" % "shiro-all" % "1.2.6",
   "org.eclipse.jdt.core.compiler" % "ecj" % "4.2.2" % "container",
   "org.apache.calcite.avatica" % "avatica" % "1.8.0",
   "org.apache.thrift" % "libthrift" % "0.9.3" pomOnly(),
   "org.apache.thrift" % "libfb303" % "0.9.3" pomOnly(),
   "org.apache.hadoop" % "hadoop-core" % "1.2.1",
   "org.apache.httpcomponents" % "httpcore" % "4.4.5",
   //"org.apache.spark" % "spark-core_2.10" % "2.2.0",
   //"org.apache.spark" % "spark-sql_2.10" % "2.2.0",
   //"org.apache.spark" % "spark-hive_2.10" % "2.2.0",
   "com.google.code.gson" % "gson" % "2.7",
   "com.sun.mail" % "javax.mail" % "1.5.1",   
   "org.scalikejdbc" %% "scalikejdbc"       % "2.2.9",
   "com.h2database"  %  "h2"                % "1.4.190",
   "ch.qos.logback"  %  "logback-classic"   % "1.1.3"
)

libraryDependencies ++= Seq(
    "org.apache.shiro" % "shiro-core" % "1.2.2",
    "org.apache.shiro" % "shiro-web" % "1.2.2",
    "com.stormpath.shiro" % "stormpath-shiro-core" % "0.6.0",
    "com.stormpath.sdk" % "stormpath-sdk-httpclient" % "1.0.RC2"
)

unmanagedJars in Compile += file("jars/Application-Data-Manipuator.jar")

unmanagedJars in Compile += file("jars/EihData_Manipulator.jar")

jetty()
