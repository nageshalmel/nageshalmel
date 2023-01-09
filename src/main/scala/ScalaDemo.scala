import com.scala.firstProject.{PostgresCommon, SparkCommon, SparkTransformer}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession


object ScalaDemo {

  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    try {
      Logger.info("Main Method started")
      val spark: SparkSession = SparkCommon.createSparkSession.get

      Logger.info("Creating a Hive Table")
//      SparkCommon.createHiveTable(spark)
      val tableName = "sampleschema.course_table"

      val courseDF = SparkCommon.readFromHiveTable(spark).get
      courseDF.show()

      val transformedDF = SparkTransformer.replaceNullValue(courseDF)
      transformedDF.show()

      PostgresCommon.writeDFToPostgres(transformedDF,tableName)


//      val sampleDF = PostgresCommon.getDataFrameFrompgTable(spark, tableName).get
//      sampleDF.show()
    } catch {
      case e:Exception =>
        Logger.error("Error in main method"+e.getMessage())
    }
  }
}
