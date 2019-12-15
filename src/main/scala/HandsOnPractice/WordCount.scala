package HandsOnPractice

import conf.SparkContextConfiguration
import org.apache.log4j.{Level, Logger}

object WordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = SparkContextConfiguration.getSparkContext("Minimum temperatures in 1800 century")
  val textFileData = sc.textFile("src/main/resources/book.txt")

  val data = textFileData.flatMap(x => x.toLowerCase.trim.split("\\W+"))

  /*or
  val data = textFileData.flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x + y).collect()
  */
  val words = data.countByValue().toSeq.sorted // Count by value returns Map which is not RDD. so better use above one to get is scalable
  /*words.foreach(println)*/

  println("*****************************************")
  textFileData.flatMap(x => x.split("\\W+")).map(x => (x,1)).reduceByKey((x,y) => x + y).sortBy(x => x._2).collect().foreach(println)
}
