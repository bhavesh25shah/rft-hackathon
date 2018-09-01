package com.hk.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object T1 {
  def main(args: Array[String]): Unit = {

    //        val runner = new BatchRunner()
    //        runner.start()
    println("Heloo......")
    try {
      val customerDataPath = "s3://abracadata//customer//customerdata.csv"

      val spark = SparkSession.builder.appName("Spark Application").getOrCreate()
      val customerDF = spark.read.format("csv").option("header", "true").load(customerDataPath)
      val count = customerDF.count()
      
      println(s"customerDF count: " + count)
      
      println(s"schema : " + customerDF.columns.mkString(","))
      
      val filterCustomerDF = customerDF.filter( customerDF.col("city").isin("Morrow", "Riverside", "Boulevard", "Boston", "Oxford") )
      
      println(s"filterCustomer count: " + filterCustomerDF.count)
      
      filterCustomerDF.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("s3://abracadata//aggr_customer//customer.csv")
      
      spark.stop()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}