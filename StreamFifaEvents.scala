import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.hadoop.hbase.client.{ ConnectionFactory, Put }
import org.apache.hadoop.hbase.util.Bytes

case class FifaEvent(
  country: String,
  new_rank: Int,
  points: Double,
  ts: Long
)

object StreamFifaEvents {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val hbaseConf: Configuration = HBaseConfiguration.create()
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("wuh_fifa_events_log"))

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: StreamFifaEvents <brokers>
           |  <brokers> is a list of one or more Kafka brokers
         """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    val sparkConf = new SparkConf().setAppName("StreamFifaEvents")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = Set("wuh_fifa_events")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "wuh_fifa_events_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SASL_SSL",
      "sasl.mechanism" -> "SCRAM-SHA-512",
      "sasl.jaas.config" -> (
        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
          "username=\"mpcs53014-2025\" password=\"Kafka password here\";"
        )
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val serializedRecords = stream.map(_.value)
    val events = serializedRecords.map(rec => mapper.readValue(rec, classOf[FifaEvent]))

    // Append-only event log: rowkey = country#ts
    val writes = events.map { ev =>
      val rowKey = s"${ev.country}#${ev.ts}"
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("e"), Bytes.toBytes("rank"), Bytes.toBytes(ev.new_rank))
      put.addColumn(Bytes.toBytes("e"), Bytes.toBytes("points"), Bytes.toBytes(ev.points))
      put.addColumn(Bytes.toBytes("e"), Bytes.toBytes("ts"), Bytes.toBytes(ev.ts))

      table.put(put)
      s"Wrote event for ${ev.country} rank=${ev.new_rank} points=${ev.points}"
    }

    writes.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
