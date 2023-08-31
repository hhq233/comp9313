package comp9313.proj2
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem1 {
    def main(args: Array[String]) {
        val k = args(0).toInt
        val stopinput = args(1)
        val input = args(2)
        val output = args(3)
        
        val conf = new SparkConf().setAppName("Problem1").setMaster("local")
        val sc = new SparkContext(conf)
        val inputFile = sc.textFile(input)
        val stopwords = sc.textFile(stopinput).collect().toArray
        val line = inputFile.map(line => line.split(",")(1))
        val words = line.map(_.split(" "))
        val words1 = words.collect
        val words2 = sc.parallelize(
        for{
            i <- 0 until words1.length
        } yield {
            words1(i).map(x => x.toLowerCase).filter(a => !stopwords.contains(a)).filter(x => x.charAt(0) <='z' && x.charAt(0) >='a')
        })
        val pairs = words2.flatMap{ words2 =>
        for{
            i <- 0 until words2.length
            j <- (i+1) until words2.length
        } yield {
            if (words2(i) < words2(j)) ((words2(i), words2(j)),1) else ((words2(j), words2(i)),1)
        }}
        val pairs1 = pairs.reduceByKey(_+_).sortBy(x => x._1).sortBy(x => x._2, false)
        sc.parallelize(pairs1.take(k).map(x => x._1._1+","+x._1._2+"\t"+x._2)).saveAsTextFile(output)
        sc.stop()
    }
}
