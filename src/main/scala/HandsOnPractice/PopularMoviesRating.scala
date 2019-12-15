package HandsOnPractice

import java.nio.charset.CodingErrorAction

import conf.SparkContextConfiguration
import org.apache.log4j.{Level, Logger}

import scala.io.{Codec, Source}

object PopularMoviesRating extends App {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines: Iterator[String] = Source.fromFile("src/main/resources/u.item").getLines()
 /*   for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }*/
    //println(movieNames)

    println("**********************")
    val map = lines.flatMap(fields => fields.split('|'))
    map.foreach(print)
    movieNames
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = SparkContextConfiguration.getSparkContext("Minimum temperatures in 1800 century")
  // Create a broadcast variable of our ID -> movie name map
  var nameDict = sc.broadcast(loadMovieNames())

  val moviesRatingData = sc.textFile("src/main/resources/u.data")
  val moviesRatings = moviesRatingData.map(x => (x.split("\t")(1).toInt,1))
  val sortedMovies =  moviesRatings.reduceByKey((x,y) => x + y)

  /* Fold in the movie names from the broadcast variable*/
  val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._1), x._2)).sortBy(x => x._2)


  // Collect and print results
  val results = sortedMoviesWithNames.collect()

  /*results.foreach(println)*/

}

