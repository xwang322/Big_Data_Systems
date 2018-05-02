import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexRDD
import scala.io.Source
import collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext._
import org.apache.spark._

//object PartBApp2Q3 {
//	def main (args: Array[String]) {
/*
		val conf = new SparkConf()
        .setAppName("CS838-Assignment3-PartB-App2-Q4")
        .setMaster("spark://10.254.0.254:7077")
        .set("spark.task.cpus", "1")
        .set("spark.executor.cores", "4")
        .set("spark.executor.memory", "16g")
        .set("spark.drive.memory", "1g")
        .set("spark.eventLog.enabled","true")
        .set("spark.eventLog.dir", "file:///home/ubuntu/logs/apps_spark_master")

        val sc = new SparkContext(conf)
*/
		val textFile1 = sc.textFile("hdfs://10.254.0.254/user/ubuntu/Assignment3PartB_data/ns_cw_200.txt")
		val textFile2 = sc.textFile("hdfs://10.254.0.254/user/ubuntu/Assignment3PartB_data/ns_cw_edge_200.txt")
		//val arrayFrom = textFile.map(_.split("\\n")).cache()
		//val vertx: RDD[(VertexId, Array[String])]
		val vertx: RDD[(VertexId, Array[String])] = textFile1.map {
			line =>
			val fields = line.split("\\s+")
			(fields(0).toLong, fields.drop(1))
		}
		vertx.persist()
		val edges: RDD[Edge[Array[String]]] = textFile2.map {
			line =>
			val parts = line.split("\\s+")
			Edge(parts(0).toLong, parts(1).toLong, parts.drop(2))
		}
		edges.persist()
		/*//constructing edges from raw file is too slow, created a separate file for edge list
		val cmpArray = textFile1.toLocalIterator.toArray
	
		val tmpedge: ArrayBuffer[Edge[Array[String]]] = new ArrayBuffer()

		for (srcLine <- cmpArray) {
			//println(e._1._1)
			for (dstLine <- cmpArray) {
				var srcWords = srcLine.split(" ")
				var dstWords = dstLine.split(" ")
				if (srcWords(0).toLong != dstWords(0).toLong) {
					println(dstWords(0).toLong)
					var tmpArr = new ArrayBuffer[String]()
					for (srcw <- srcWords.drop(1)) {
						for (dstw <- dstWords.drop(1)) {
							if (srcw == dstw && srcw != "") {
								tmpArr += srcw
								//println(tmpList.size)
							}
						}
					}
					if (tmpedge.size > 0)
						tmpedge += new Edge(srcWords(0).toLong, dstWords(0).toLong, tmpArr.toArray)
					println(tmpedge.size)
				}
			}
		}
		
		println(tmpedge.toArray.size)
		
		val edges: RDD[Edge[Array[String]]] = sc.parallelize(tmpedge)
		*/
		
		val graph = Graph(vertx, edges).cache

		var arrayBuf: ArrayBuffer[String] = new ArrayBuffer()
		
		val r = graph.triplets.collect.foreach(line => (line.attr.foreach(e => (arrayBuf += e))))
		
		val array = arrayBuf.toArray

		val wordsRDD: RDD[String] = sc.parallelize(array)

		val counts = wordsRDD.map(word => (word, 1)).reduceByKey {case (x, y) => x + y}.map(line => (line._1, line._2/2))

		val res = counts.sortBy(_._2, false).take(10)
		
		println(s"Assignment3 Part-B Application-2 Question 4")
		println(s"Top 10 most popular words")
		res.foreach(println)
//	}
//}

