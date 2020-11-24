package cn.edu.nju

import java.nio.charset.StandardCharsets

import cn.edu.nju.po.UserCartoonRate
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization

object ww{
  def main(args: Array[String]) = {
    val s = """{"uid": "66868349", "cid": "\udccd\udcbb\udcbb\udcf7\udcc0\udcf2\udcc0\udcf2 BOUQUET", "score": "4\u661f"}"""
    val s2 = """{"uid": "56143385", "cid": "\udcb1\udcf0\udcb2\udce1\udcb0\udcc2\udcc1\udcd6\udcc6\udca5\udcbf\udccb\udcd6\udcae\udcbb\udcb7", "score": "4\u661f"}"""
    val s3 = """{"uid": "zyy310568680", "cid": "\udcb9\udce2\udcd6\udcae\udcd5\udcbd\udcbc\udcc7\udca3\udcadZUERST\udca3\udcad\udca3\udca8\udcd4\udcd6\udcbb\udcf6\udcb5\udcc4\udcd5\udce6\udcc0\udced\udca3\udca9", "score": "5\u661f"}"""
    val s4 = """{"uid": "140091927", "cid": "Healin' Good #U2661 #U5149#U4e4b#U7f8e#U5c11#U5973", "score": 5}
               |""".stripMargin
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val userCartoon =  parse(s4).extract[UserCartoonRate]
    println(userCartoon.id)
    println(userCartoon.cid)
    println(userCartoon.score)
    val sw = "你好"
    print(new String(sw.getBytes(), StandardCharsets.UTF_8))
  }

}
