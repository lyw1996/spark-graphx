import java.io.{BufferedReader, InputStreamReader}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sparkGraphx {
  val conf = new SparkConf().setAppName("Draw Graph").setMaster("local")
  val sc = new SparkContext(conf)


  def makeGraph(vertexsFile:String,edgesFile:String):Graph[String,Long] = {
    val words = sc.textFile(vertexsFile)
    val weight = sc.textFile(edgesFile)


    val word:RDD[(VertexId,String)] = words.map{
      line =>
        val row = line.split(" ")
        (row(0).toLong,row(1))
    }
    val adjacencyWeight :RDD[Edge[Long]] = weight.map{
      line =>
        val row = line.split(" ")
        Edge(row(0).toLong,row(1).toLong,row(2).toLong)
    }

    val graph:Graph[String,Long] = Graph(word,adjacencyWeight)
    return graph
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val vertexsFile = "F:\\simulate-hdfs\\vertex.txt"
    val edgesFile = "F:\\simulate-hdfs\\edge.txt"
    //    val characters = sc.textFile("/Users/liuyang/Downloads/vertex(1).txt")
    //    val times = sc.textFile("/Users/liuyang/Downloads/edge(1).txt")
    //get the graph use graphx
    val xgraph = makeGraph(vertexsFile, edgesFile)
    println("请输入你想要输入的字：")
    //    val content=Console.readLine()
    //
    val br = new BufferedReader((new InputStreamReader(System.in)))
    val content = br.readLine()
    //    val x=content.substring(content.length()-1)
    //    val content=Console.readLine()

    var inter_arr:Long=0
    var inter_tri=new EdgeTriplet[String,Long]
    var inter_value1=new String

    def init_aword(content:String):EdgeTriplet[String,Long]= {
      for (triplet <- xgraph.triplets.filter(t => t.srcAttr == content).collect()) {
        if (triplet.attr > inter_arr) {
          inter_arr = triplet.attr
          inter_tri = triplet
        }
      }
      inter_tri.srcAttr=content
      return inter_tri
    }
    var i:Long=0
    def aword(a:EdgeTriplet[String,Long]):EdgeTriplet[String,Long]={
      //      println("第"+i+"次")
      val y=a.srcAttr
      //        .substring(a.srcAttr.length-1)
      inter_arr=0
      inter_value1=""
      for (triplet <- xgraph.triplets.filter(t => (t.srcAttr == y) &&(a.srcAttr.contains(t.dstAttr)==false) && (t.attr>5)).collect()) { //t.arr可以设置成大于想要的权值
        if (triplet.attr > inter_arr) {
          inter_arr = triplet.attr
          inter_value1=triplet.dstAttr
        }
      }
      a.srcAttr+=inter_value1
      if(inter_value1==""&&i<2)
        return a
      else {
        if(i<2) {
          i=i+1
          //          println(a.srcAttr)
          return aword(a)}
        else return  a
      }

    }
    val end_tri=aword(init_aword(content))

    //    for(ver<-xgraph.triplets.collect()) {
    //      val end_tri = aword(init_aword(ver.srcAttr))
    //      println(end_tri.srcAttr)
    //    }


    //inter_value:结果
    var interarr=new Array[Long](3)
    var inter_value=new Array[String](3)
    for(triplet<-xgraph.triplets.filter(t=>t.srcAttr==content).collect()){
      if (triplet.attr > interarr(0) ) {
        inter_value(2)=inter_value(1)
        inter_value(1)=inter_value(0)
        inter_value(0) = triplet.dstAttr
        interarr(0) = triplet.attr
      }
      else if (triplet.attr > interarr(1)) {
        inter_value(2)=inter_value(1)
        inter_value(1) = triplet.dstAttr
        interarr(1) = triplet.attr
      }
      else if (triplet.attr > interarr(2)) {
        inter_value(2) = triplet.dstAttr
        interarr(2) = triplet.attr
      }
    }
    //输出联想词
    var a=0
    for(a<- 0 to 2){
      if(inter_value(a)!=null) {
        println(content + inter_value(a))
      }
    }
    // println(end_tri.srcAttr)
  }



}
