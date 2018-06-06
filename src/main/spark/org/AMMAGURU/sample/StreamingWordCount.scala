package org.AMMAGURU.sample

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


object StreamingWordCount {
  def main(args : Array[String]): Unit = {
    
    /*
     * Change the log level in AMMA_GURU_Spark_Scala_Java/src/main/resources/log4j.properties
     * to ERROR (log4j.rootCategory=ERROR, S) for better readability
     */
    
    println("Om Hari Sri Ganapathaye Namaha \n Avighnamasthu Sri Guruvey Namaha")
    println("AMMA GURU")
    
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCount")
    //val sparkConf = new SparkConf().setAppName("StreamingWordCount")
    
    val streamingContext =  new StreamingContext(sparkConf,Seconds(1))
    
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = streamingContext.socketTextStream("localhost",9999)
    
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    
    // Print the first ten elements of each RDD generated in this DStream to the console
    print("AMMMMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    wordCounts.print()
    
    streamingContext.start()             // Start the computation
    streamingContext.awaitTermination()  // Wait for the computation to terminate
    
  }
 
}

