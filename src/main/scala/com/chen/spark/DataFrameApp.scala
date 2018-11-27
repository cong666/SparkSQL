package com.chen.spark

import org.apache.spark.sql.SparkSession


object DataFrameApp {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    //transfer json to DataFrame
    val peopleDF = spark.read.format("json").load("/Users/ccong/Documents/J2EEProject/SparkTestData/people.json")

    //print schema information
    peopleDF.printSchema()

    //show 20 first records
    peopleDF.show()

    //select name from table
    peopleDF.select("name").show()

    // select all data from column ,  select name, age+10 as age2 from table
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()

    //column filter : select * from table where age>19
    peopleDF.filter(peopleDF.col("age") > 19).show()

    //group： select age,count(1) from table group by age
    peopleDF.groupBy("age").count().show()

    spark.stop()
  }

}
