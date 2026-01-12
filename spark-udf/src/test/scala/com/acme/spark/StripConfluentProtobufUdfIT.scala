package com.acme.spark

import scala.jdk.CollectionConverters._

import com.acme.demo.v1.DemoEvent
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test

class StripConfluentProtobufUdfIT {

  @Test
  def stripConfluentProtobufUdf_worksInSparkSql(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("StripConfluentProtobufUdfIT")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      spark.udf.register("strip_confluent", new StripConfluentProtobufUdf(), DataTypes.BinaryType)

      val expected = DemoEvent
        .newBuilder()
        .setId("id-123")
        .setCreatedAtEpochMs(1700000000000L)
        .setPayload("hello from test")
        .build()

      val schemaRegistryClient = new MockSchemaRegistryClient()
      val serializer = new KafkaProtobufSerializer[DemoEvent](schemaRegistryClient)
      serializer.configure(
        Map(
          "schema.registry.url" -> "mock://strip-udf-it",
          "auto.register.schemas" -> "true"
        ).asJava,
        false
      )

      val framed = serializer.serialize("demo.protobuf", expected)
      assertNotNull(framed)

      import spark.implicits._
      val df = Seq(framed).toDF("value")

      val stripped = df.selectExpr("strip_confluent(value) as payload").collect()(0).getAs[Array[Byte]](0)
      val actual = DemoEvent.parseFrom(stripped)

      assertEquals(expected, actual)
    } finally {
      spark.stop()
    }
  }
}
