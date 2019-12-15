package HandsOnPractice

import conf.SparkContextConfiguration
import org.apache.log4j.{Level, Logger}

object MinTemperatuers extends App {

  def parseLines(lines : String) ={
    val data = lines.split(",")
    val stationId = data(0)
    val entryType = data(2)
    val temperature = data(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId,entryType,temperature)
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = SparkContextConfiguration.getSparkContext("Minimum temperatures in 1800 century")
  val textFileData = sc.textFile("src/main/resources/1800.csv")
  val dataRDD = textFileData.map(parseLines) // This will return tuple of (stationId,entryType,temperature) for each lines fed from RDD to map.
  val minTemp =  dataRDD.filter(x => x._2 == "TMIN")
  // Now extract only StationId and its Temperature
  val stationTemps = minTemp.map(x => (x._1,x._3.toFloat))
    .reduceByKey((x,y) => Math min(x, y)).collect()

  stationTemps.foreach(println)





}
