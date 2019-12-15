package HandsOnPractice

import conf.SparkContextConfiguration
import org.apache.log4j.{Level, Logger}

object CustomerExpenses extends App {

  def parseInput(customerData : String) = {
    val splittedData = customerData.split(",")
    (splittedData(0).toInt,splittedData(2).toFloat)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = SparkContextConfiguration.getSparkContext("Minimum temperatures in 1800 century")
  val csvFileData = sc.textFile("src/main/resources/customer-Order.scv")
  val cIdAndAmountMap = csvFileData.map(parseInput).reduceByKey((x,y) => x + y).collect().sortBy(x => x._2)
  cIdAndAmountMap.foreach(println)

}
