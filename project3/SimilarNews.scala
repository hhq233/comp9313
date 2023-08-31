package comp9313.proj3
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimilarNews {
    def main(args: Array[String]) {
        val inputFile = args(0)
        val outputFolder = args(1)
        val t = args(2).toDouble
        val conf = new SparkConf().setAppName("SimilarNews").setMaster("local")
        val sc = new SparkContext(conf)
        val textFile = sc.textFile(inputFile)
        // make a word list in least frequency
        val wordFreq = textFile.map(line => line.split(","))
                               .filter(x=>x.length>1)
                               .map(x=>x(1))
                               .flatMap(_.split(" "))
                               .map(x => (x,1))  // word -> (word,1)
                               .reduceByKey(_+_) // reduce by key
                               .sortByKey()
                               .sortBy(x => x._2) // sort by value
                               .map{case(x,y)=>x} // (word, 1) -> word
                               .collect
        val headword = textFile.map(line => line.split(",")).zipWithIndex()
                               .filter(x=>x._1.length>1)
                               .map{case(x, y)=>((y,x(0).take(4)), x(1).split(" "))} // ((rid,year), word array)     
                               .map{case(x, y)=>(x, y.sortBy(x=>wordFreq.indexOf(x)))} //sort words      
                               .collect
        val tokenpair = headword.flatMap{case(x,y) =>
        for {
            // j <- 0 to (listlength * (1-t)+1 or just listlength)
            j <- 0 until ( if (((y.length * (1-t)).round.toInt + 1) > y.length) y.length else ((y.length * (1-t)).round.toInt + 1))
        }
        yield {
            (y(j),(x,y)) // yield (token, ((rid, year), words))
        }}.sortBy(x=>x._1)
        val similarpair = (
        for {
            // two pairs should have same token but different rids and years
            // yield ((rid1, rid2), similarity)
            i <- 0 until tokenpair.length
            j <- i+1 until tokenpair.length
            if (tokenpair(i)._1 == tokenpair(j)._1 
                && tokenpair(i)._2._1._1 != tokenpair(j)._2._1._1 
                && tokenpair(i)._2._1._2 != tokenpair(j)._2._1._2)
        } yield {                      
            (
             (tokenpair(i)._2._1._1.toInt, tokenpair(j)._2._1._1.toInt),                    
              tokenpair(i)._2._2.intersect(tokenpair(j)._2._2).distinct.length.toDouble/
              tokenpair(i)._2._2.union(tokenpair(j)._2._2).distinct.length.toDouble) 
        })
        sc.parallelize(similarpair).filter(x=>x._2>=t)
                                   .reduceByKey((a,b)=>a)
                                   .sortByKey()
                                   .map(x=>x._1+"\t"+x._2)
                                   .saveAsTextFile(outputFolder)
        sc.stop()
    }
}
