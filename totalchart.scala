
import org.graphstream.graph.{Graph => GraphStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.graphx.Graph
import org.graphstream.graph.implementations.SingleGraph
import org.graphstream.graph.implementations.SingleNode
import org.graphstream.graph.implementations.AbstractEdge
object totalchart {
  def draw(srcGraph:Graph[String,Long]): Unit ={
    val vigraph: SingleGraph = new SingleGraph("graphDemo")
    vigraph.setAttribute("ui.stylesheet", "F:\\simulate-hdfs\\sheet.txt")
    vigraph.setAttribute("ui.quality")
    vigraph.setAttribute("ui.antialias")
    //    load the graphx vertices into GraphStream
    for ((id, value) <- srcGraph.vertices.collect()) {
      val node = vigraph.addNode(id.toString).asInstanceOf[SingleNode]
      node.addAttribute("ui.label", value.toString)
    }

    //    load the graphx edges into GraphStream edges
    for (Edge(x, y, _) <- srcGraph.edges.collect()) {
      val edge = vigraph.addEdge(x.toString + ',' + y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
    }

    vigraph.display()

  }
  //  def createGraph(vertexsFile:String,edgesFile:String):Graph[String,Long] = {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("GraphStreamDemo")
      .set("spark.master", "local[*]")

    val sc = new SparkContext(sparkConf)
    val characters = sc.textFile("D:\\我的文档\\Tencent Files\\369310624\\FileRecv\\edge3.txt")
    val times = sc.textFile("D:\\我的文档\\Tencent Files\\369310624\\FileRecv\\vertex3.txt")


    val character: RDD[(VertexId, String)] = characters.map {
      line =>val row = line.split(" ")
        (row(0).toLong, row(1))
    }
    val adjacencyTimes: RDD[Edge[Long]] = times.map {
      line =>
        val row = line.split(" ")
        Edge(row(0).toLong, row(1).toLong, row(2).toLong)
    }

    //    val graph: Graph[String, Long] = Graph(character, adjacencyTimes)
    val srcGraph = Graph(character, adjacencyTimes)
    //    return graph
    //  }

     draw(srcGraph)

  }
}



