package com.chen.spark

import org.apache.spark.sql.SparkSession;
/**
  * Created by: ccong 
  * Date: 18/11/29 上午12:32
  */
object SparkParquet {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("SparkParquet").master("local").getOrCreate()

    //by defaut sparksql use parquet format by default
    //val userDF = sc.read.load("/Users/ccong/Documents/J2EEProject/SparkTestData/users.parquet")
    val userDF = sc.read.format("parquet").load("/Users/ccong/Documents/J2EEProject/SparkTestData/users.parquet")

    userDF.printSchema()

    userDF.select("name","favorite_color","favorite_numbers").show()

    sc.stop()
  }
}
