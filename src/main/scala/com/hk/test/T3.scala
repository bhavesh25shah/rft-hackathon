package com.hk.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object T3 {
  def main(args: Array[String]): Unit = {

    //        val runner = new BatchRunner()
    //        runner.start()
    println("Heloo......")
    try {
      val customerDataPath = "s3://abracadata//aggr_customer//customer.csv"
      val mortgageDataPath = "s3://abracadata//aggr_mortgage//mortgage.csv"

      val spark = SparkSession.builder.appName("Spark Application").getOrCreate()
      val mortgageDF = spark.read.format("csv").option("header", "true").load(mortgageDataPath)
      val customerDF = spark.read.format("csv").option("header", "true").load(customerDataPath)
      val mCount = mortgageDF.count()
      val cCount = mortgageDF.count()
      
      println(s"DF count: " + mCount + " " + cCount)
     
      println(s"mortgage schema : " + mortgageDF.columns.mkString(","))
      println(s"customer schema : " + customerDF.columns.mkString(","))
      
      val resultDf = mortgageDF.join(customerDF, customerDF.col("customerid") === mortgageDF.col("borrowerid"))
      
      println(s"Result count: " + resultDf.count)
      
      resultDf.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("s3://abracadata//result//customer_mortgage.csv")
      
      spark.stop()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}