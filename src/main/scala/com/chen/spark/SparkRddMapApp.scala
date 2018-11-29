package com.chen.spark

import org.apache.spark.sql.SparkSession;

/**
  * Created by: ccong 
  * Date: 18/11/29 下午9:00
  */
object SparkRddMapApp {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("SparkRddActionApp").master("local").getOrCreate()
    val lines = sc.sparkContext.textFile("/Users/ccong/Documents/J2EEProject/SparkTestData/hellospark.txt")
    /*
    * Key : the first word of the line
    * Value : the whole line
    * (hello,hello world)
      (hello,hello spark)
      (!,!)
      (help,help me)
      (!,!)
    */
    val mapData = lines.map(line=>(line.split(" ")(0),line))
    mapData.foreach(println)


    /*
     Create the map from array
    * */
    val rddData = sc.sparkContext.parallelize(Array((1,2),(3,4),(3,6)))
    rddData.foreach(println)
    /*
      Reduce by key with the same type
      sum the value for the elements which have the same key
      (1,2)
      (3,10)
    */
    val reducedRddData = rddData.reduceByKey((x,y)=>x+y)
    println("reduceByKey")
    reducedRddData.foreach(println)

    /*
     (1,CompactBuffer(2))
     (3,CompactBuffer(4, 6))
    */
    val groupByKeyRdd = rddData.groupByKey()
    println("groupByKey")
    groupByKeyRdd.foreach(println)

    /*
    * mapValues , change value
    * value+1
    * */
    val mapValuesRdd = rddData.mapValues(y=>y+1)
    println("mapValues")
    mapValuesRdd.foreach(println)

    /*
    * flatMapValues()
    * */

    val flatMapValuesRdd = rddData.flatMapValues(y=>y to 5)
    println("flatMapValues")
    flatMapValuesRdd.foreach(println)


    /*
    * keys , values
    * */
    val keysRdd = rddData.keys
    val valuesRdd = rddData.values
    println("keysRdd")
    keysRdd.foreach(println)
    println("valuesRdd")
    valuesRdd.foreach(println)

    /*
    * sortByKey
    * */
    val sortByKeyRdd = rddData.sortByKey()
    println("sortByKey")
    sortByKeyRdd.foreach(println)

    sc.stop()
  }
}
