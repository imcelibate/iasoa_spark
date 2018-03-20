package org.AMMAGURU.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object DemoSpark {

  def main(args: Array[String]) = {
    println("AMMA GURU")

    val sparkConf = new SparkConf().setAppName("My AMMA Spark").setMaster("local[1]").set("spark.executor.memory", "1g");

    val sparkContext = new SparkContext(sparkConf)

    //Print from Array
    val strArray = Array("jan", "feb", "mar", "april", "may", "jun")
    val rdd1 = sparkContext.parallelize(strArray)
    val result = rdd1.coalesce(2)
    result.foreach(println)
    //Print from Array
    val strSeq = Seq(("maths", 52), ("english", 75), ("science", 82), ("computer", 65), ("maths", 85))
    val data = sparkContext.parallelize(strSeq)
    val sorted = data.sortByKey()
    sorted.foreach(println)

    //Print after reading file from HDFS
    //Cache and persist
    val ipText = sparkContext.textFile("hdfs://localhost:54310/data/mapreduce/sampleData/UnitUsageSample.txt")
    val words = ipText.flatMap(line => (line.split(" ")))
    words.cache();
    words.persist(StorageLevel.MEMORY_ONLY)
    words.foreach(println)
    
    //Map test
    val lineLength = ipText.map(line => (line, "--> Char length :"+line.length()))
    lineLength.foreach(println)
    
     //Flat Map test
    val lineitem = ipText.flatMap(line => line.split(" "))
    lineitem.foreach(println)
    
     //Filter test - incomplete
    println("AMMA :  even numbers")
    val lineItemEven = lineitem.flatMap( eachItem => {
                    eachItem.filter(value => (value.toInt)%2 == 0)
      
                }
        )
    lineItemEven.foreach(println)
    
  }

}