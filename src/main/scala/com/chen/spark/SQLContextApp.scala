package com.chen.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
/**
  * Created by: ccong 
  * Date: 18/11/27 下午10:16
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {

    val path = args(0)

    val sparkConf = new SparkConf();
    sparkConf.setAppName("SQLContextApp").setMaster("local");
    //create SparkContext
    val sparkContext = new SparkContext(sparkConf);
    //create SQLContext
    val sqlContext = new SQLContext(sparkContext);
    //do job
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()
    //close resouce
    sparkContext.stop();
  }
}
