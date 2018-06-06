package org.AMMAGURU.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
    def main(args : Array[String])= {
      
      println("Om Hari Sri Ganapathaye Namaha \nAvighnamasthu Sri Guruve Namaha")
      
      println("OM AMMA")
      
     val sparkConf = new SparkConf().setAppName("My AMMA Spark")//.setMaster("local[1]").set("spark.executor.memory","1g");
     
     val sparkContext = new SparkContext(sparkConf)
      
      val ipText = sparkContext.textFile("/home/imcelibate/AMMA/tech/BIGData/SparkJars/sample_data/SparkWordCount.txt",0)
      
      val words = ipText.flatMap(line => (line.split(" ")))
      
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      
      // Save the word count back out to a text file, causing evaluation.
      counts.saveAsTextFile("/home/imcelibate/AMMA/tech/BIGData/SparkJars/op")
      
      sparkContext.stop()
      
      
    }
}