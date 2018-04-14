package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object wordCount {
  def main(args:Array[String])={
    val conf=new SparkConf()
    .setAppName("Word Count")
    .setMaster("local")
    
    val sc=new SparkContext(conf)
    val test=sc.textFile("Example.txt")
    val data=test.flatMap{line=>line.split(" ")}
    val datacount=data.map{word=>(word,1)}
    
    val redcount=datacount.reduceByKey(_+_)
    redcount.saveAsTextFile("Details.txt")
  }
}