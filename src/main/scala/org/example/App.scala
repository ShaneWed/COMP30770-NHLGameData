package org.example

import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]): Unit = {
    println("Initializing SparkConf & SparkContext")
    val conf = new SparkConf()
      .setAppName("sparkTest")
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)

    val file = "/home/shane/Documents/UCD/COMP30770/project/extracted/game_skater_stats.csv"
    println("Reading file")
    val rddPrime = sc.textFile(file).take(100)
    rddPrime.take(2).foreach(println)

    val rddSplit = rddPrime.map(line => line.split(","))
    rddSplit.take(5).foreach(println)
    rddSplit.take(5).foreach(columns => println(columns.mkString(", ")))

    sc.stop()
  }
}
