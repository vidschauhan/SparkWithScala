package basics

import org.apache.spark.{SparkConf, SparkContext}

/*
"@author : Vidit Singh"
"Date" : 29/8/19
*/
object GettingStarted  extends App {

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)

  // Load the text into a Spark RDD, which is a distributed representation of each line of text
  val textFile = sc.textFile("src/main/resources/zookeeper.out")

  //word count This also works --> val counts = textFile.flatMap(line => line.split(" ")).countByValue()
  val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)
  System.out.println("Total words: " + counts.count());
  counts.saveAsTextFile("/tmp/shakespeareWordCount")
}
