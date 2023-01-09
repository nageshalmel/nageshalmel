package com.scala.firstProject


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util.Properties

object PostgresCommon {

  private val Logger = LoggerFactory.getLogger(getClass.getName)

  Logger.info("Setting properties for postgres")
  def getPostgresCommonProperties: Properties = {
    val pgConnectionProperties = new Properties()

    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","nagesh")
    Logger.info("user and properties are set")
    pgConnectionProperties
  }

  def getPostgresServerDatabase : String = {
    Logger.info("getPostgresServerDatabase() started")

    val pgURL = "jdbc:postgresql://localhost:5432/Nagesh"
    pgURL
  }

  def getDataFrameFrompgTable(spark: SparkSession, pgTable: String): Option[DataFrame] = {
    try{
      Logger.info("Establishing connection to postgres databse")
      val pgTableDF = spark.read.jdbc(getPostgresServerDatabase,pgTable,getPostgresCommonProperties)
      Logger.info("Fteching data from database")
      Some(pgTableDF)
    }catch {
      case e:Exception =>
        Logger.error("Exception in a getdatframe method"+e.printStackTrace())
        System.exit(0)
        None
    }

  }

  def writeDFToPostgres(dataFrame: DataFrame,pgTable: String): Unit = {
    try{
      dataFrame.write
        .mode(SaveMode.Append)
        .format("jdbc")
        .option("url",getPostgresServerDatabase)
        .option("dbtable",pgTable)
        .option("user","postgres")
        .option("password","nagesh")
        .save()
      Logger.info("Dataframe is written to postgres table")
    }catch {
      case e:Exception =>
        Logger.error("Exception in a getdatframe method"+e.printStackTrace())
        System.exit(0)
        None
    }
  }

}
