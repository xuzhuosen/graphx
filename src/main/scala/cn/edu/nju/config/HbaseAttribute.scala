package cn.edu.nju.config

object HbaseAttribute {
  val userCartoonTableName = "userCartoon"
  val columnFamilyOfUserCartoon = "score"

  val relativeUsersTableName = "relativeUsers"
  val columnFamilyOfRelativeUsers = "relative"
  val columnOfRelativeUsers = "neighbors"

  val userActiveTableName = "userActive"
  val columnFamilyOfUserActive = "activenum"
  val columnOfUserActive = "active"



}
