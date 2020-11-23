package cn.edu.nju

import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, Get, Put, Scan, TableDescriptorBuilder}

import scala.collection.mutable.ArrayBuffer

object HbaseUtil {
  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.rootdir", "hdfs://namenode:9000/hbase")
  val connection: Connection = ConnectionFactory.createConnection(conf)
  val admin: Admin = connection.getAdmin

  def tableIsExist(tableName: String): Boolean = {
    admin.tableExists(TableName.valueOf(tableName))
  }

  def createTable(tableName: String, columnFamilys: Array[String]): Unit = {
    if (!tableIsExist(tableName)) {
      val descriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
      for (col <- columnFamilys) {
        descriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(col.getBytes()).build())
      }
      admin.createTable(descriptor.build())

      println("create table " + tableName)
    }
    else {
      println("table exists!")
    }
  }

  def dropTable(tableName: String): Unit = {
    admin.disableTable(TableName.valueOf(tableName))
    admin.deleteTable(TableName.valueOf(tableName))

    println("drop table " + tableName)
  }

  def insertRow(tableName: String, rowkey: String, columnFamily: String, column: String, value: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val puts = new Put(rowkey.getBytes())

    puts.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes())
    table.put(puts)

    println(s"insert table: ${tableName}  rowkey: ${rowkey}  colFam: ${columnFamily}  col: ${column}  val: ${value}")
  }

  def getDataByRowKeyAndCol(tableName: String, rowKey: String, columnFamilys: String, column: String): String = {
    val table = connection.getTable(TableName.valueOf(tableName))

    val get = new Get(rowKey.getBytes())
    get.addColumn(columnFamilys.getBytes(), column.getBytes())

    val result = table.get(get)
    if (result != null)
      bsToString(result.getValue(columnFamilys.getBytes(), column.getBytes()))
    else ""
  }
  //[(uid, (Array[(cid, score)])]
  def scanTableColumn(tableName: String, columnFamily: String): Array[(String, Array[(String, Int)])] = {
    val table = connection.getTable(TableName.valueOf(tableName))

    val scan = new Scan()
    scan.addFamily(columnFamily.getBytes())

    val scanner = table.getScanner(scan)

    val ret = ArrayBuffer[(String, Array[(String, Int)])]()
    var result = scanner.next()
    while(result != null) {
      val uid = bsToString(result.getRow)
      val cidScoreArray = ArrayBuffer[(String, Int)]()
      val cols = result.getFamilyMap(columnFamily.getBytes())
      cols.forEach((c, v) => {cidScoreArray.append((bsToString(c), bsToString(v).toInt))})
      ret.append((uid, cidScoreArray.toArray))
      result = scanner.next()
    }
    ret.toArray
  }

  def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
        println("close hbase connection")
      } catch {
        case e: Exception => println(e)
      }
    }
  }

  def bsToString(bs: Array[Byte]): String = {
    new String(bs, StandardCharsets.UTF_8)
  }

}
