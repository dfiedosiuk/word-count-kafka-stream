import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.util.Properties

object WordCount extends App {

  import org.apache.kafka.streams.scala._
  import ImplicitConversions._
  import serialization.Serdes._

  type Line = String
  val builder = new StreamsBuilder
  builder
    .stream[String, Line]("ks-input")
    .flatMapValues { value => value.split("\\W+") }
    .groupBy { case (_, value) => value}
    .count()
    .mapValues(_.toString)
    .toStream
    .to("ks-output")

  val topology: Topology = builder.build()

  import scala.concurrent.duration._

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app_id")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":9092")
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5.seconds.toMillis)
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
  val ks = new KafkaStreams(topology, props)

  ks.start()

}
