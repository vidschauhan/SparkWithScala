package HandsOnPractice

import basics.GettingStarted.sc
import conf.SparkContextConfiguration
import org.apache.log4j.{Level, Logger}

/*
"@author : Vidit Singh"
"Date" : 01/9/19
*/
object FriendsAgeAvg extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  def getFriendsAndAge(lines : String) ={
    val data = lines.split(",")
    val age = data(2).toInt
    val friends = data(3).toInt
    (age,friends)
  }
  val sc = SparkContextConfiguration.getSparkContext("Friends Age Average")
  val textFileData = sc.textFile("src/main/resources/dummyFriends.csv")

  //Feeding each line of RDD "textFileData" to map which is passed to getFriendsAndAge
  // This will return Key,value RDD
  val ageAndFriendsRDD = textFileData.map(getFriendsAndAge)
  val friendsNumAndCount = ageAndFriendsRDD.mapValues(x => (x,1)) // This will convert Previous RDD to (key,(friendsNum,1)
  //This will add friendsNumber as well as its count respectively in a tuple for Unique Keys.
  // ReduceByKey takes two values of the **same** key and adds them up here. syntax doesn't represent (k,v)
  val totalByAge = friendsNumAndCount.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))

  //Avg now
  val avgFriendsByAge = totalByAge.mapValues(x => x._1 / x._2)
  val finalOutput = avgFriendsByAge.collect() // Action is triggered now DAG will be executed.

  finalOutput.sorted.foreach(println)




}
