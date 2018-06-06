package org.AMMAGURU.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
import org.apache.spark.TaskContext

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
    println("Print after reading file from HDFS :  connecting to HDFS : ")
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
    println(s"wordcount1 RDD \n : ${wordcount1}")
    wordcount1.foreach(println)
    val wordcount2 = wordcount1.reduceByKey(_+_)
        wordcount2.foreach(println)
        
    //Combine by test    
    val sampleIp = Seq(
                       ("AMMA" , 1) , 
                       ("KALI",9),  
                       ("MATHA", 7), 
                       ("GURU", 2),   
                       ("AMMACHI",4) , 
                       ("PONAMMA",5), 
                       ("AMMA", 9),
                       ("AMMAGURU", 10),
                       ("GURU", 99),
                       ("AMMA", 97),
                       ("GURU", 77),
                       ("AMMACHI", 4444)
                      )
    val  ipRDD = sparkContext.parallelize(sampleIp)    
    
    println("Count By Value for a Paid RDD"+ ipRDD.countByValue())
        
    val input = ipRDD.combineByKey(
         (myAmma) => {
           println(s"Create combiner -> ${myAmma}")
           (myAmma, 1)
         },
         (acc: (Int, Int), v) => {
          println(s"""Merge value : (${acc._1} + ${v}, ${acc._2} + 1)""")
           (acc._1 + v, acc._2 + 1)
         },
         (acc1: (Int, Int), acc2: (Int, Int)) => {
           println(s"""Merge Combiner : (${acc1._1} + ${acc2._1}, ${acc1._2} + ${acc2._2})""")
           (acc1._1 + acc2._1, acc1._2 + acc2._2)
         }
        )         
    input.foreach(println)            
    val avr = input.map(item => println(s"when map : ${item}")).cache()    
    val avr2 = input.mapValues(item => println(s"when mapValues : ${item}")).cache()  
    avr.collect();
    avr2.collect();
    
    val avrage1way = input.map(item => (item._1, item._2._1/item._2._2.toFloat)) 
    avrage1way.persist(StorageLevel.DISK_ONLY)
    println("average way 1 using Map")
    avrage1way.foreach(println) 
    val avrage2ndWay = input.mapValues(item => item._1/item._2.toFloat)    
    println("average way 2 using MapValues")    
    avrage2ndWay.foreach(println) 
    
        //Aggregetate test  
    val  aggRDD = sparkContext.parallelize(sampleIp,3) 
    val aggRes = aggRDD.aggregate(0)(
                   /*
                   |     * This is a seqOp for merging T into a U
                   |     * ie (String, Int) in  into Int
                   |     * (we take (String, Int) in 'value' & return Int)
                   |     * Arguments :
                   |     * acc   :  Reprsents the accumulated result
                   |     * value :  Represents the element in 'inputrdd'
                   |     *          In our case this of type (String, Int)
                   |     * Return value
                   |     * We are returning an Int
                   |     *
                   */
                      (acc , value) => (acc + value._2),
         /*
          * This is a combOp for mergining two U's
          * (ie 2 Int)
          */
                      
                      (acc1, acc2) => (acc1 + acc2)
                      )
        
    println(s"Aggregate : ${aggRes}")
    
    //Reduce test  
    val  redRDD = sparkContext.parallelize(sampleIp,3) 
    val redRslt = redRDD.reduce(
      (x,y) => (x._1+""+y._1,x._2+y._2)
      )
    
    println("Reduce Pair RDD: "+ redRslt)
    
        //Fold test  
      val marks = Seq(
                       ("MATHS", 10),
                       ("Physics", 10),
                       ("Chem", 10),
                       ("BIO", 10)
                      )
    val  foldRDD = sparkContext.parallelize(marks,3) 
    TaskContext.getPartitionId()
    println(s"Partition count for foldRDD : ${foldRDD.partitions.size}")
    val foldRslt = foldRDD.fold("graceMark", 5){
          (x,y) => 
           val total = x._2 + y._2
           println(s"in Partition -  ${TaskContext.getPartitionId()} x.2 --> : ${x._2} , y.2 : ${y._2}")
           println(s"total --> : ${total}")
           ("total",total)
       }
    
    println("Fold Pair RDD: "+ foldRslt)
    
        //top test     
      /*  val sampleIp = Seq(
                       ("AMMA" , 1) , 
                       ("KALI",9),  
                       ("MATHA", 7), 
                       ("GURU", 2),   
                       ("AMMACHI",4) , 
                       ("PONAMMA",5), 
                       ("AMMA", 9),
                       ("AMMAGURU", 10),
                       ("GURU", 99),
                       ("AMMA", 97),
                       ("GURU", 77),
                       ("AMMACHI", 4444)
                      )*/
    
    val  topRecRDD = sparkContext.parallelize(sampleIp) 
    val top3RecRdd = topRecRDD.top(3)
    println("Print all elements ....")
    topRecRDD.foreach(println)
    println("Print top 3 ....")
    top3RecRdd.foreach(println)
    
    //run sort a rdd
    
    //lookup test
    val  lookupRDD = sparkContext.parallelize(sampleIp) 
    val rsltLkpRdd = lookupRDD.lookup("AMMA")
    rsltLkpRdd.foreach(println)
    println(s"check rsltLkpRdd instance of ${rsltLkpRdd.isInstanceOf}")
    
        //toDebug String test
    val  toDebugRDD = sparkContext.parallelize(sampleIp) 
    println("toDebugString test")    
    toDebugRDD.foreach(println)
    toDebugRDD.toDebugString(2)
    println("2nd Print for to debugString test")  
    toDebugRDD.foreach(println)
    
    
    //Joins, left outer , right outer join test
    val joinRdd1 = sparkContext.parallelize(Seq(
      ("math",    55),
      ("math",    56),
      ("english", 57),
      ("english", 58),
      ("science", 59),
      ("science", 54)))
      
     val joinRdd2 = sparkContext.parallelize(Seq(
      ("math",    55),
      ("math",    56),
      ("history", 57),
      ("history", 58),
      ("science", 59),
      ("science", 54)))
      
      val innerJoinRDD = joinRdd1.join(joinRdd2)
      println("innerJoinRDD : ")
      innerJoinRDD.foreach(println)
      val lOutterJoinRDD = joinRdd1.leftOuterJoin(joinRdd2)
      println("Left OutterJoinRDD : ")
      lOutterJoinRDD.foreach(println)
      val rOutterJoinRDD = joinRdd1.rightOuterJoin(joinRdd2)
      println("Right OutterJoinRDD : ")
      rOutterJoinRDD.foreach(println)
      val fOutterJoinRDD = joinRdd1.fullOuterJoin(joinRdd2)
      println("Full OutterJoinRDD : ")
      fOutterJoinRDD.foreach(println)
      
      val coGroupRDD = joinRdd1.cogroup(joinRdd2)
      println("co-Group RDD : ")
      coGroupRDD.foreach(println)
      
      
          //run sort a rdd      
      val sortRdd = sparkContext.parallelize(sampleIp)
      println("Sort example : Before sort: ")
      sortRdd.foreach(println)
      
      val ascSort = sortRdd.sortByKey()
      println("Sort example : After asc  sort: ")
      ascSort.foreach(println)
      
      val descSort = sortRdd.sortByKey(false)
      println("Sort example : After Desc  sort: ")
      descSort.foreach(println)
      
     //Partitioner test 
      val partTestIP = Seq(
                       ("MATHS", 10),
                       ("Physics", 10),
                       ("Chem", 10),
                       ("BIO", 10)
                      )
    val  partTestRdd = sparkContext.parallelize(partTestIP,3) 
    TaskContext.getPartitionId()
    println(s"Partition count for parition test : ${partTestRdd.partitions.size}")
    
    
    
    
  }

}