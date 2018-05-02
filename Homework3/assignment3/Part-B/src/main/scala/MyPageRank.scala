import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.GraphLoader


object MyPageRank {
    def main(args: Array[String]) {

        if (args.length < 1 || args.length > 2) {
            System.err.println("Usage: MyPageRank <file> <iter>")
            System.exit(1)
        }

        val numIter = if (args.length > 1) args(1).toInt else 20

        val resetProb = 0.15

        val initialMessage = 0.0

        val conf = new SparkConf()
        .setAppName("CS838-Assignment3-PartB-App1")
        .setMaster("spark://10.254.0.254:7077")
        .set("spark.task.cpus", "1")
        .set("spark.executor.cores", "4")
        .set("spark.executor.memory", "16g")
        .set("spark.drive.memory", "1g")
        .set("spark.eventLog.enabled","true")
        .set("spark.eventLog.dir", "file:///home/ubuntu/logs/apps_spark_master")

        val sc = new SparkContext(conf)

        val graph = GraphLoader.edgeListFile(sc, args(0))

        val tmp = graph.outerJoinVertices(graph.outDegrees) {
            (vid, vdata, deg) => deg.getOrElse(0)
        }
        //tmp.vertices.take(10)

        val edgetmp = tmp.mapTriplets(e => 1.0/e.srcAttr)
        //edgetmp.triplets.take(5)

        val initialGraph = edgetmp.mapVertices( (id, attr) => 1.0)
        //initialGraph.vertices.take(10)

        // val initialGraph: Graph[Double, Double] = graph.outerJoinVertices(graph.outDegrees) {
        //     (vid, vdata, deg) => deg.getOrElse(0)
        // }
        // .mapTriplets(e => 1.0/e.srcAttr)
        // .mapVertices((id, attr) => 1.0)

        def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
            resetProb + (1.0 - resetProb) * msgSum

        def sendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
            Iterator((edge.dstId, edge.srcAttr * edge.attr))

        def messageCombiner(a: Double, b: Double): Double = a + b

        val pagerankGraph = Pregel(initialGraph, initialMessage, numIter) (
            vertexProgram, sendMessage, messageCombiner)
        

        //initialGraph.vertices.take(10)

		pagerankGraph.vertices.collect.foreach(println) //{
	

		sc.stop()
	}



}
