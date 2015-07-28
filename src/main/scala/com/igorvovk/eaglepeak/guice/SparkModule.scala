package com.igorvovk.eaglepeak.guice

import com.google.inject.{AbstractModule, Inject, Provider, Singleton}
import com.typesafe.config.Config
import net.codingwell.scalaguice.ScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SparkModule {

  def commonConfig() = {
    val commonSettings = Map(
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.executor.memory" -> "2g",
      "spark.driver.memory" -> "2g"
    )

    new SparkConf()
      .setAppName("EaglePeek")
      .setAll(commonSettings)
  }

  class TestSparkContextProvider extends Provider[SparkContext] {

    override def get() = {
      new SparkContext(commonConfig().setMaster("local"))
    }

  }

  class SparkContextProvider @Inject()(config: Config) extends Provider[SparkContext] {

    override def get() = {
      val config = commonConfig()
        .setMaster("local[2]") // TODO: from configuration

      new SparkContext(config)
    }
  }

  class SQLContextProvider @Inject()(sparkContext: SparkContext) extends Provider[SQLContext] {

    override def get() = new SQLContext(sparkContext)

  }

}

class SparkModule(test: Boolean = false) extends AbstractModule with ScalaModule {

  import SparkModule._

  override def configure(): Unit = {
    if (test) {
      bind[SparkContext].toProvider[TestSparkContextProvider]
      bind[SQLContext].toProvider[SQLContextProvider]
    } else {
      bind[SparkContext].toProvider[SparkContextProvider].in[Singleton]
      bind[SQLContext].toProvider[SQLContextProvider].in[Singleton]
    }
  }
}
