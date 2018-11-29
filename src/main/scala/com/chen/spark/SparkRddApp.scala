package com.chen.spark

import org.apache.spark.sql.SparkSession;

/**
  * Created by: ccong 
  * Date: 18/11/29 下午9:00
  */
object SparkRddApp {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("SparkRddApp").master("local").getOrCreate()
    val lines = sc.sparkContext.textFile("/Users/ccong/Documents/J2EEProject/SparkTestData/hellospark.txt")
    lines.foreach(println)
    println("lines count : "+lines.count())

    /*return another rdd with filter
    * return the line which contains world
    * */
    val linesReturnedByFilter = lines.filter(line=>line.contains("world"))
    println("**lines with world**")
    linesReturnedByFilter.foreach(println)

    /*
    * How to use Map
    * return a new rdd with map()
    * (hello,1)
      (spark,1)
      (hello,1)
      (world,1)
      (!,1)
    * */
    val linesFromArray = sc.sparkContext.parallelize(Array("hello","spark","hello","world","!"))
    val newMap = linesFromArray.map(word=>(word,1))
    println("**Use of map**")
    newMap.foreach(println)

    /*flatMap()
     *return all words split by space
     * */
    val dataForFlatMap = sc.sparkContext.textFile("/Users/ccong/Documents/J2EEProject/SparkTestData/hellospark.txt")

    var dataFromFlatMap = dataForFlatMap.flatMap(line=>line.split(" "))
    println("**FlatMap testing**")
    dataFromFlatMap.foreach(println)

    /*
    * rdd union and intersection
    * */

    val rddTest1 = sc.sparkContext.parallelize(Array("coffe","coffe","panda","monkey","tea"))
    val rddTest2 = sc.sparkContext.parallelize(Array("coffe","monkey","kitty"))
    //delete the element repeated
    val rddTest1Distinct = rddTest1.distinct()
    println("**distinct**")
    rddTest1Distinct.foreach(println)

    val rddUnion = rddTest1.union(rddTest2)
    println("**rdd union**")
    rddUnion.foreach(println)

    val rddIntersection = rddTest1.intersection(rddTest2)
    println("**rdd intersection**")
    rddIntersection.foreach(println)

    /*
    *
    * tea
      panda*/
    val dataInTest1NotInTest2 = rddTest1.subtract(rddTest2)
    println("**substract**")
    dataInTest1NotInTest2.foreach(println)

    /*create rdd using SparkContext from a Array*/
    val rdd = sc.sparkContext.parallelize(Array(1,2,2,4),4)
    println("Number of elements in RDD : "+rdd.count())
    //print the rdd 's elements in random order
    rdd.foreach(print)

    /*create rdd from seq*/

    val dataFromSeq=sc.sparkContext.parallelize(Seq(("sun",1),("mon",2),("tue",3), ("wed",4),("thus",5)))
    val dataSorted = dataFromSeq.sortByKey()
    println("rdd from seq and sorted")
    dataSorted.foreach(println)

    sc.stop()
  }
}
