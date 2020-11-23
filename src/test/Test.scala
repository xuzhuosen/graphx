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
//package main.java;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import main.scala.HbaseUtil;
//
//import java.util.Arrays;
//import java.util.Properties;
//
//public class DataConsumer {
//  static final String userCartoonsTableName = "userCartoon";
//  static final String columnFamils = "dataColumnFamily";
//  static final String userCartoonsColumn = "score";
//
//  public static void main(String[] args) {
//    Properties props = new Properties();
//    props.put("bootstrap.servers", "node1:9092");
//    props.put("group.id", "test");
//    props.put("enable.auto.commit", "true");
//    props.put("auto.commit.interval.ms", "1000");
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//    consumer.subscribe(Arrays.asList("zhuosen"));
//    while (true) {
//      ConsumerRecords<String, String> records = consumer.poll(100);
//      for (ConsumerRecord<String, String> record : records) {
//        String rowkey = record.key();
//        String rate = record.value();
//        //String beforeRate = HbaseUtil.getDataByRowKeyAndCol(userCartoonsTableName, rowkey, columnFamils, userCartoonsColumn);
//        HbaseUtil.insertRow(userCartoonsTableName, rowkey, columnFamils, userCartoonsColumn, rate);
//      }
//      if(records.isEmpty()) break;
//    }
//
//  }
//}
