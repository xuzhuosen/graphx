package cn.edu.nju
import java.time.Duration
import java.util

import org.json4s.{Formats, NoTypeHints}
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import org.apache.hadoop.hbase.AuthUtil
import cn.edu.nju.config.HbaseAttribute
import cn.edu.nju.po.UserCartoonRate
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
object DataConsumer {


  def consumer(group: String="test3") = {
    val props = new Properties()
    props.put("bootstrap.servers", "node2:9092")
    props.put("group.id", group)
    props.put("enable.auto.commit", "false")
    props.put("auto.offset.reset","earliest")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("zhuosen"))
    val records = consumer.poll(Duration.ofMillis(1000)).asScala
    while (records.nonEmpty) {
      for (record <- records) {
        implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
        val userCartoon =  parse(record.value).extract[UserCartoonRate]
        HbaseUtil.insertRow(HbaseAttribute.userCartoonTableName, userCartoon.id, HbaseAttribute.columnFamilyOfUserCartoon, userCartoon.cid, userCartoon.score)
      }
      consumer.commitSync()
    }
  }

}
