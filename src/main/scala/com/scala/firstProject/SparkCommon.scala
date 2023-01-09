package com.scala.firstProject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object SparkCommon {
  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession: Option[SparkSession] = {
    //System.setProperties("hadoop.home.dir", "C:\\hadoop")

    try {
      Logger.info("Creating SparkSession")
      val spark = SparkSession
        .builder()
        .appName("HelloSpark")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()

      Logger.info("Spark Session created")
      Some(spark)
    } catch {
      case e: Exception =>
        Logger.error("exception while creating spark session" + e.printStackTrace())
        System.exit(1)
        None
    }
  }

  def createHiveTable(spark: SparkSession): Unit = {
    Logger.warn("Creating Local Hive table in not exists")
    spark.sql("create database if not exists Nagesh")
    spark.sql("create table if not exists Nagesh." +
      "course_table(course_id string, course_name string," +
      "author_name string, no_of_rating string)")
    spark.sql("insert into Nagesh.course_table VALUES (1,'Java','FutureX',45)")
    spark.sql("insert into Nagesh.course_table VALUES (2,'Java','FutureXSkill',56)")
    spark.sql("insert into Nagesh.course_table VALUES (3,'Big Data','Future',100)")
    spark.sql("insert into Nagesh.course_table VALUES (4,'Linux','Future',100)")
    spark.sql("insert into Nagesh.course_table VALUES (5,'Microservices','Future',100)")
    spark.sql("insert into Nagesh.course_table VALUES (6,'CMS','',100)")
    spark.sql("insert into Nagesh.course_table VALUES (7,'Python','FutureX','')")
    spark.sql("insert into Nagesh.course_table VALUES (8,'CMS','Future',56)")
    spark.sql("insert into Nagesh.course_table VALUES (9,'Dot Net','FutureXSkill',34)")
    spark.sql("insert into Nagesh.course_table VALUES (10,'Ansible','FutureX',123)")
    spark.sql("insert into Nagesh.course_table VALUES (11,'Jenkins','Future',32)")
    spark.sql("insert into Nagesh.course_table VALUES (12,'Chef','FutureX',121)")
    spark.sql("insert into Nagesh.course_table VALUES (13,'Go Lang','',105)")

    // Treat empty strings as Null
    spark.sql("alter table Nagesh.course_table set tblproperties('serialization.null.format'='')")
  }

  def readFromHiveTable(spark: SparkSession): Option[DataFrame] = {
    try {
      Logger.info("Reading data from Hive Table")
      val sampleDF = spark.sql("select * from Nagesh.course_table")
      Logger.info("read table is stored as dataframe")
      Some(sampleDF)
    } catch {
      case e: Exception => Logger.error("exception while reading from Hive table")
        None
    }

  }
}
