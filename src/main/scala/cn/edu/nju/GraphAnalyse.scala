package cn.edu.nju

import cn.edu.nju.config.HbaseAttribute
import cn.edu.nju.po.UserRelative
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import org.json4s.{Formats, NoTypeHints}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization

object GraphAnalyse {
  val maxrate = 10
  val maxRelativeUserNum = 6

  def main(args: Array[String]): Unit={
    //test()
    //(uid, cid, score)
//    val s = System.getProperty("java.classpath")
//    println(s"System property"+s)
    val command = args(0)
    if(command.contains("consumer")) {
      if(args.length>=2)
        DataConsumer.consumer(args(1))
      else
        DataConsumer.consumer()
    }
    else if(command.contains("analyse")){
      //[(uid, Array[(cid, score)])]
      val arrayMsg: Array[(String, Array[(String, Int)])] = HbaseUtil
        .scanTableColumn(HbaseAttribute.userCartoonTableName, HbaseAttribute.columnFamilyOfUserCartoon)
      arrayMsg.foreach( a => {print(s"arrayMsg:  uid:${a._1}  cid:${a._2}")})
      val graph: Graph[String, Int] = buildGraph(arrayMsg)

      val userActives: Array[(String, Int)] = getUserActives(graph)
      userActives.foreach(userActive => HbaseUtil.insertRow(HbaseAttribute.userActiveTableName, userActive._1,  HbaseAttribute.columnFamilyOfUserActive, HbaseAttribute.columnOfUserActive, userActive._2.toString))

      implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
      arrayMsg.map(_._1).foreach(uid => {
        val sortedRelativeUser: Array[UserRelative] = getSortedRelativeUsers(uid, graph)
        HbaseUtil.insertRow(HbaseAttribute.relativeUsersTableName, uid,
          HbaseAttribute.columnFamilyOfRelativeUsers, HbaseAttribute.columnOfRelativeUsers, write(sortedRelativeUser))
      })
    }
  }

  def buildGraph(arrayMsg: Array[(String, Array[(String, Int)])]): Graph[String, Int] = {
    val conf = new SparkConf()
      .setAppName("graphx analyse")
    //.setMaster("spark://namenode:7077")
    val sc = new SparkContext(conf)

    var maxSize = arrayMsg.length+1

    val cidArray = sc.parallelize(arrayMsg).map(_._2).flatMap(arr => arr.map(_._1).toSeq).distinct().zipWithIndex()collect()
    var cidToVidMap: Map[String, Long] = Map()
    cidArray.foreach(a => {cidToVidMap += a._1 -> (a._2+maxSize)})
    cidToVidMap.foreach(s11 => print(s"cidToMid: cid: ${s11._1} -> vid: ${s11._2} ;"))
    //val cidToVidMap = sc.broadcast(cidToIndexMap)

    val vertexs = ArrayBuffer[(VertexId, String)]()
    val edges = ArrayBuffer[Edge[Int]]()
    //((uid, (Array[(cid, score)]), index)
    arrayMsg.zipWithIndex.foreach(e => {
      vertexs.append((e._2, e._1._1))
      e._1._2.foreach(
        cidAndScore => {
          vertexs.append((cidToVidMap(cidAndScore._1), cidAndScore._1))
          edges.append(Edge(e._2, cidToVidMap(cidAndScore._1), cidAndScore._2))
        }
      )
    })
    edges.foreach(e => printf(s"edges: ${e.srcId} -> ${e.dstId}  val: ${e.attr} "))
    val usersAndCartoons: RDD[(VertexId, String)] = sc.parallelize(vertexs)
    val relations: RDD[Edge[Int]] = sc.parallelize(edges)
    val defaultV =  "defaultUser"
    val graph = Graph(usersAndCartoons, relations, defaultV)
    graph
  }
  def getUserActives(graph: Graph[String, Int]): Array[(String, Int)] = {
    val userActives: VertexRDD[(String, Int)] = graph.aggregateMessages(trip => {
      trip.sendToSrc((trip.srcAttr, 1))
    },
      (m1, m2) => (m1._1, m1._2+m2._2)
    )
    userActives.map(_._2).collect()
  }

  def getSortedRelativeUsers(uid: String, graph: Graph[String, Int]): Array[UserRelative] = {
    val cartoonsSeenIdAndRate: Array[(VertexId, Int)] = graph.triplets
      .filter(t => t.srcAttr==uid).map(t => (t.dstId, t.attr)).collect()
    val cartoonsSeenIds: Array[VertexId] = cartoonsSeenIdAndRate.map(_._1)
    val subg = graph.subgraph(epred = edge => cartoonsSeenIds.contains(edge.dstId))
    //[vid, (String, Int)]
    val sortedUser: VertexRDD[(String, Int)] = subg.aggregateMessages(trip => {
      val r1 = trip.attr
      val cartoonId = cartoonsSeenIds.indexOf(trip.dstId)
      val r2 = cartoonsSeenIdAndRate(cartoonId)._2
      val ur = maxrate - Math.abs(r1 - r2)
      trip.sendToSrc(trip.srcAttr, ur)
    },
      (m1, m2) => (m1._1, m1._2+m2._2)
    )
    var ret: Array[(String, Int)] = sortedUser.sortBy(_._2._2, ascending = false).map(_._2).collect()
    if(ret.length > maxRelativeUserNum) ret = ret.slice(1, maxRelativeUserNum)
    else ret = ret.slice(1, ret.length)
    ret.map(ur => UserRelative(ur._1, ur._2))
  }

  //  def test() = {
  //        val uid = 1L
  //        val conf = new SparkConf()
  //          .setAppName("first spark app(scala)")
  //          .setMaster("local");
  //
  //        val sc = new SparkContext(conf)
  //
  //        val maxrate = 5
  //
  //        val usersAndCartoons: RDD[(VertexId, String)] = sc.parallelize(Seq((1L, "u1"), (2L, "u2"), (3L, "m1"), (4L, "m2")));
  //        val cartoons: RDD[(VertexId, String)] = sc.parallelize(Seq((3L, "m1"), (4L, "m2")));
  //        val relations: RDD[Edge[Int]] = sc.parallelize(Seq(Edge(1L, 3L, 4),Edge(2L, 3L, 3), Edge(2L, 4L, 1)))
  //        val defaultV =  (("d"))
  //        val graph = Graph(usersAndCartoons, relations, defaultV)
  //        graph.vertices.foreach(v => print(v._2))
  //  }
}
