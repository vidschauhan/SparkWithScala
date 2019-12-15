package conf

import org.apache.spark.sql.SparkSession

/*
Returns spark session object.
 */
object SparkSessionConf {

  def getSparkSession(appName : String ) : SparkSession =  SparkSession.builder
    .appName(appName)
    .master("local[*]")
    .getOrCreate()
}
