import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import scala.jdk.CollectionConverters.PropertiesHasAsScala

object KafkaUtil {

  val saslJaasConfigTemplate: String =
    "org.apache.kafka.common.security.plain.PlainLoginModule required    username=\"%s\"    password=\"%s\";"

  /**
   * Builds connection properties for the Kafka client
   *
   * @param brokers        the Kafka brokers
   * @param username       the username for the Kafka instance
   * @param password       the password for the username
   * @param topic          the topic name
   * @param startingOffset the starting offset from which to read the data
   * @return the connection properties
   */
  def buildKafkaConnectionProperties(
                                      brokers: String,
                                      username: String,
                                      password: String,
                                      topic: String,
                                      startingOffset: String = "earliest",
                                      endingOffset: String = "latest"
                                    ): Map[String, String] = {
    val saslJaasConfig: String = saslJaasConfigTemplate.format(username, password)

    val props: Properties = new Properties()
//    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
//    props.put(
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
//      classOf[StringDeserializer].getCanonicalName
//    )
    props.put("subscribe", topic)
//    props.put("assign", s"{\"${topic}\":[0]}")
//    props.put("startingOffsets", s"{\"${topic}\":{\"0\":14886991 }}")
//    props.put("endingOffsets", s"{\"${topic}\":{\"0\":14887000 }}")

    props.put("kafka.bootstrap.servers", brokers)
    props.put("startingOffsets", startingOffset)
    props.put("endingOffsets", endingOffset)
    props.put("enable.auto.commit", "true")
    props.put("failOnDataLoss", "false")
    props.put("kafka.security.protocol", "SASL_SSL")
    props.put("kafka.sasl.mechanism", "PLAIN")
    props.put("kafka.sasl.jaas.config", saslJaasConfig)
    props.asScala.toMap
  }
}
