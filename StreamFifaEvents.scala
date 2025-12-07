import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
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

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println("Usage: StreamFifaEvents <broker:port>")
      System.exit(1)
    }

    val brokers = args(0)

    // -----------------------------
    // Spark Streaming Context
    // -----------------------------
    val sparkConf = new SparkConf().setAppName("StreamFifaEvents")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topics = Set("wuh_fifa_events")

    // -----------------------------
    // Kafka Configuration
    // -----------------------------
    val groupId = s"wuh_fifa_group_${System.currentTimeMillis()}"

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      // use earliest to see existing messages in the topic
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SASL_SSL",
      "sasl.mechanism" -> "SCRAM-SHA-512",
      "sasl.jaas.config" ->
        """org.apache.kafka.common.security.scram.ScramLoginModule required
          |username="mpcs53014-2025"
          |password="A3v4rd4@ujjw";""".stripMargin
    )

    println(s"Using Kafka consumer group: $groupId")

    // -----------------------------
    // Kafka DStream
    // -----------------------------
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val serialized = stream.map(_.value)
    serialized.print()   // DEBUG: raw Kafka messages

    val events = serialized.map(rec => mapper.readValue(rec, classOf[FifaEvent]))
    events.print()       // DEBUG: parsed event objects

    // -----------------------------
    // Process and write to HBase
    // -----------------------------
    val writes = events.mapPartitions { partitionIter =>

      if (!partitionIter.hasNext) {
        // nothing in this micro-batch
        Iterator.empty
      } else {

        // Load HBase configuration inside executor
        val hConf = HBaseConfiguration.create()

        // Load Spark-distributed copy (if present)
        hConf.addResource(new Path("hbase-site.xml"))

        // ALSO load system-wide EMR config
        hConf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))

        // Debug print — verifies Spark loaded actual EMR HBase config
        println("===== HBASE CONFIG CHECK =====")
        println("hbase.zookeeper.quorum = " +
          hConf.get("hbase.zookeeper.quorum", "<NULL>"))
        println("hbase.zookeeper.property.clientPort = " +
          hConf.get("hbase.zookeeper.property.clientPort", "<NULL>"))
        println("zookeeper.znode.parent = " +
          hConf.get("zookeeper.znode.parent", "<NULL>"))
        println("================================")

        try {
          // Create HBase connection
          val connection = ConnectionFactory.createConnection(hConf)
          val logTable = connection.getTable(TableName.valueOf("wuh_fifa_events_log"))
          val liveTable = connection.getTable(TableName.valueOf("wuh_fifa_team_stats"))

          val results = partitionIter.map { ev =>

            // 1. Event log row
            val rowKey = s"${ev.country}#${ev.ts}"
            val putLog = new Put(Bytes.toBytes(rowKey))
            putLog.addColumn(Bytes.toBytes("e"), Bytes.toBytes("rank"), Bytes.toBytes(ev.new_rank))
            putLog.addColumn(Bytes.toBytes("e"), Bytes.toBytes("points"), Bytes.toBytes(ev.points))
            putLog.addColumn(Bytes.toBytes("e"), Bytes.toBytes("ts"), Bytes.toBytes(ev.ts))
            logTable.put(putLog)

            // 2. Live table update
            val putLive = new Put(Bytes.toBytes(ev.country))
            putLive.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("latest_live_rank"), Bytes.toBytes(ev.new_rank))
            putLive.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("latest_live_points"), Bytes.toBytes(ev.points))
            putLive.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("latest_live_ts"), Bytes.toBytes(ev.ts))
            liveTable.put(putLive)

            s"✔ Updated LIVE for ${ev.country}: rank=${ev.new_rank}, points=${ev.points}"
          }.toList

          connection.close()
          results.iterator
        } catch {
          case e: Throwable =>
            // Make the *real* HBase error visible in the Spark logs
            e.printStackTrace()
            Iterator.single(s"✘ HBase write failed: ${e.getClass.getName}: ${e.getMessage}")
        }
      }
    }

    writes.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
