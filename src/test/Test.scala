package test

import main.scala.po.UserRelative
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

//case class UserRelative(uid: String, relevance: Int) extends Serializable
object Test {
  def main(args: Array[String]): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)
    val array: Array[UserRelative] = Array(UserRelative("1",1), UserRelative("2",1), UserRelative("3",4))
    print(write(array))
  }

}
