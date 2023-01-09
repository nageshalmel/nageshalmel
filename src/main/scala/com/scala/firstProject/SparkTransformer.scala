package com.scala.firstProject

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

object SparkTransformer {
  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def replaceNullValue(dataframe: DataFrame): DataFrame ={
    Logger.info("filling null author_names value with UNKNOWN value ")
    val transformedDF = dataframe.na.fill("UNKNOWN",Seq("author_name"))
      .na.fill("0",Seq("no_of_rating"))
    Logger.info("Filled no_of_rating null values with 0 ")

    transformedDF

  }


}
