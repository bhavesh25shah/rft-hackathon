package com.hk.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object T2 {
  def main(args: Array[String]): Unit = {

    //        val runner = new BatchRunner()
    //        runner.start()
    println("Heloo......")
    try {
      val mortgageDataPath = "s3://abracadata//mortgage//mortgagedata.csv"

      val spark = SparkSession.builder.appName("Spark Application").getOrCreate()
      val mortgageDF = spark.read.format("csv").option("header", "true").load(mortgageDataPath)
      val count = mortgageDF.count()
      
      println(s"mortgageDF count: " + count)
      
      println(s"schema : " + mortgageDF.columns.mkString(","))
      
      val filtermortgageDF = mortgageDF.filter( mortgageDF.col("principal") > 2000000.00 )
      
      println(s"filtermortgage count: " + filtermortgageDF.count)
      
      filtermortgageDF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("s3://abracadata//aggr_mortgage//mortgage.csv")
      
      spark.stop()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}