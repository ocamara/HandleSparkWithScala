import org.apache.spark._
import org.apache.spark.streaming._ // not necessary since Spark 1.3

/**
  * Created by ousminho on 10/12/17.
  */


object Streaming {

  def test()=
  {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    println("demarrage")
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()             // Start the computation

    println ("attente de nouvelles lignes ")

    ssc.awaitTermination()  // Wait for the computation to termin
  }

}
