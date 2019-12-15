package conf

import org.apache.spark.SparkContext
/*
"@author : Vidit Singh"
"Date" : 01/9/19
*/
object SparkContextConfiguration {

  /**
   * Returns Spark context on runtime.
   * @param appName
   * @return
   */
  def getSparkContext(appName: String) = new SparkContext("local[*]", appName)
}
