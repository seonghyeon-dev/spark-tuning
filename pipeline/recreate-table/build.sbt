// 1회성 Spark 앱: Iceberg 테이블 재생성 + tmp_id(NOT NULL) 추가
//
// 버전은 운영 스택(pipeline/s3fileio-migration-guide.md §5.0.2 실측)에 맞춘다.
//   Spark 3.5.8 / Scala 2.12 / Iceberg 1.10.1
// Spark·Iceberg는 이미지의 $SPARK_HOME/jars 가 제공하므로 provided,
// ojdbc8 은 이 앱만 쓰는 라이브러리이므로 fat jar 에 포함한다 (배치 원칙: 가이드 §5.0.5).

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "lakehouse"
ThisBuild / version      := "0.1.0"

val sparkVersion   = "3.5.8"
val icebergVersion = "1.10.1"
val ojdbcVersion   = "23.9.0.25.07"   // ojdbc8: JDK 8/11/17/21 호환

lazy val root = (project in file("."))
  .settings(
    name := "recreate-table",
    libraryDependencies ++= Seq(
      "org.apache.spark"          %% "spark-sql"                  % sparkVersion   % Provided,
      "org.apache.iceberg"        %  "iceberg-spark-runtime-3.5_2.12" % icebergVersion % Provided,
      "com.oracle.database.jdbc"  %  "ojdbc8"                     % ojdbcVersion
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint"),

    // sbt-assembly: 산출물 이름 고정 (K8s 매니페스트의 mainApplicationFile 과 일치)
    assembly / assemblyJarName := "recreate-table-assembly.jar",
    assembly / mainClass := Some("RecreateTable"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF")                 => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.exists(_.endsWith(".SF")) ||
                                            xs.exists(_.endsWith(".DSA")) ||
                                            xs.exists(_.endsWith(".RSA")) => MergeStrategy.discard
      case PathList("META-INF", "services", _*)            => MergeStrategy.concat
      case PathList("META-INF", _*)                         => MergeStrategy.first
      case "module-info.class"                                  => MergeStrategy.discard
      case _                                                    => MergeStrategy.first
    },
    // provided 의존성은 assembly 에서 제외되지만 `sbt run` 시엔 필요 없음 (spark-submit 전용)
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)
  )
