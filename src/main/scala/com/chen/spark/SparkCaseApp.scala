package com.chen.spark

import org.apache.spark.sql.SparkSession;

/**
  * Created by: ccong 
  * Date: 18/11/29 下午9:00
  */
object SparkCaseApp {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("SparkCaseApp").master("local").getOrCreate()
    val lines = sc.sparkContext.textFile("/Users/ccong/Documents/J2EEProject/SparkTestData/people.data")

    import sc.implicits._
    val peopleDF = lines.map(_.split(",")).map(word=>People(word(0).toInt,word(1).toInt,word(2),word(3),word(4))).toDF()

    peopleDF.printSchema()

    /*
    * +---+---+---+-------------+--------+
      | id|age|sex|          job|identify|
      +---+---+---+-------------+--------+
      |  1| 24|  M|   technician|   85711|
      |  2| 53|  F|        other|   94043|
      |  3| 23|  M|       writer|   32067|
      |  4| 24|  M|   technician|   43537|
    * */
    peopleDF.show()

    peopleDF.sort("age").show(40)

    peopleDF.sort(peopleDF("age").desc,peopleDF("sex").asc).show

    peopleDF.groupBy(peopleDF("job")).count().sort("count").show()

    peopleDF.filter("job=='none'").show()
    sc.stop()
  }

  case class People(id: Int, age: Int, sex: String, job: String, identify: String)
}
