package HandsOnPractice

import conf.SparkContextConfiguration
import org.apache.log4j.{Level, Logger}

object MostPopularSuperHero extends App {

  def marvelHeroes(lines : String) : Option[(Int,String)] = {
   var data = lines.split('\"')
    if(data.length > 1)
      Some(data(0).trim.toInt,data(1))
    else None
  }

  def parseGraphData(lines : String) ={
    val fields = lines.split("\\s+") // Splitting data from whitespaces eg. space,tab
    (fields(0).toInt,fields.length - 1)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = SparkContextConfiguration.getSparkContext("Most popular super hero")
  val superHeroNamesData = sc.textFile("src/main/resources/Marvel-names.txt") // Loading marvel hero names in RDD
  val mappedData = superHeroNamesData.flatMap(marvelHeroes)

  val marvelGraphData = sc.textFile("src/main/resources/Marvel-graph.txt") // Loading marvel graph Data

  val graphData = marvelGraphData.map(parseGraphData)
  val numberOfAppearances = graphData.reduceByKey((x,y) => x + y)
  val flippedData = numberOfAppearances.map( x => (x._2,x._1)).max()
  val mostPopularNames = mappedData.lookup(flippedData._2).head
  println(s"$mostPopularNames is the most popular hero with ${flippedData._1}")




}
