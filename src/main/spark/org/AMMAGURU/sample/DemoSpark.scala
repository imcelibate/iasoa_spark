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
    println("Flat Map TEst : ")
    lineitem.foreach(println)
    
    println("AMMA : Filter even numbers")
    val lineItemFilter = lineitem.flatMap( eachItem => eachItem.split("\t").filter(value => (value.toInt)%2 == 0))
    println("RDD Count: "+lineItemFilter.count())
    lineItemFilter.foreach(println)
    
    //Count by Value test
    println("AMMA : Count by Value test")
    val countByValueTest = lineItemFilter.countByValue();
    countByValueTest.foreach(println)
    
    
    
//     //Filter test - incomplete
//    println("AMMA :  even numbers")
//    val lineItemEven = lineitem.flatMap( eachItem => {
//                    eachItem.filter(value => (value.toInt)%2 == 0)
//      
//                }
//        )
//    lineItemEven.foreach(println)
    

    
    //Map Partition
    val lineitem1 = ipText.mapPartitions{
      (iterator) => {
                         //  println("Called in Partition -> " + index)
                         // In a normal user case, we will do the
                         // the initialization(ex : initializing database)
                         // before iterating through each element
                         val myList = iterator.toList
                         myList.map(x => x + " -> " + "AMMA").iterator
                       }
    }
    lineitem1.foreach(println)

        //Map Partition
    val lineitem2 = ipText.mapPartitionsWithIndex{
      (index,iterator) => {
                         println("Called in Partition -> " + index)
                         // In a normal user case, we will do the
                         // the initialization(ex : initializing database)
                         // before iterating through each element
                         val myList = iterator.toList
                         myList.map(x => x + " -> " + "AMMA" + index).iterator
                       }
    }
    lineitem2.foreach(println)
    
    //union rdd test
    println("Union Test")
    val rddUnion = words.union(lineitem)
    rddUnion.foreach(println)
    
     //intersection rdd test
    println("intersection Test")
    val rddIntersection = words.intersection(lineitem)
    rddIntersection.foreach(println)

    //distinct rdd test
    println("distinct Test")
    val rdd2 = sparkContext.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014),(3,"nov",2014)))
    val result1 = rdd2.distinct()
    println(result1.collect().mkString(", "))
 
    //groupByKey rdd test  
    println("groupByKey Test")
    val data1 = sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)))
    val group = data1.groupByKey()
    group.foreach(println)
    
    //collect Action test
    println("collect Action Test")
    val lineitem3 = ipText.flatMap(line => line.split(" "))  
    val lineItemArray = lineitem3.collect()
    println("Size in Array : "+ lineItemArray.size)
    println("Item 1 : "+ lineItemArray(0))
    println("Size after split  : "+ lineItemArray(0).toString().split("\t").size)  
    lineItemArray.foreach(println)

    //reduce action test
    println("reduce Action Test")
    val exArray = Array("AMMA", "KALI", "MATHA", "GURU", "AMMACHI", "PONAMMA","AMMA","AMMAGURU", "GURU")
    val  wordCount = sparkContext.parallelize(exArray)
    val wordcount1 =  wordCount.map(word => (word,1))
    val wordcount2 = wordcount1.reduceByKey(_+_)
        wordcount2.foreach(println)
    
    
  }

}