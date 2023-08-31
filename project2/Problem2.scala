package comp9313.proj2

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2 {
    def main(args: Array[String]) = {
        val conf = new SparkConf().setAppName("Problem2").setMaster("local")
        val sc = new SparkContext(conf)
        val inputfile = args(0) 
        val output = args(1)
        val edges = sc.textFile(inputfile)   
   
        val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, 0.0))	
        val graph = Graph.fromEdges[Double, Double](edgelist, 0.0)
	    graph.triplets.collect().foreach(println)
        val k = graph.vertices.collect.size // length of vertices
        val initialGraph = graph.mapVertices((id, _) => Set[VertexId]())
        val res = initialGraph.pregel(Set[VertexId](),k)(
            (id, ns, newns) => ns ++ newns, // Vertex Program
            triplet => {  // Send Message
            Iterator((triplet.dstId, triplet.srcAttr + triplet.srcId))
        },
        (a, b) => a++b // Merge Message
        )
       
        // [node 0 0, node 1 0, node 2 0 ...]
        val pairs1 = res.vertices.map{x => (x._1, 0)}
        // [node 0 [0,1,2,...], node 1 [0,1,2,..]...]
        val pairs2 = res.vertices.flatMap{case(id, attr) => 
            for {i <- 0 until attr.toArray.length
                 val a = attr.toArray } 
            yield {(a(i),1)}// count the nodes can reach this node
            }.reduceByKey(_+_).sortByKey()
        // merge and sum the result
        val pairs3 = pairs1.cogroup(pairs2).
                     map{case(x, y)=>(x,(y._1++y._2).sum)}.                     
                     sortByKey().
                     map(x=>x._1+":"+x._2).saveAsTextFile(output)
        
        sc.stop()	
    }
}
