package com.igorvovk.eaglepeak.guice

import com.google.inject.{AbstractModule, Inject, Provider, Singleton}
import com.typesafe.config.Config
import net.codingwell.scalaguice.ScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SparkModule {

  val name = "EaglePeek"

  class TestSparkContextProvider extends Provider[SparkContext] {

    override def get() = new SparkContext("local", name)

  }

  class SparkContextProvider @Inject()(config: Config) extends Provider[SparkContext] {

    override def get() = {
      val config = new SparkConf()
        .setMaster("local[2]") // TODO: from configuration
        .setAppName(name)

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
    } else {
      bind[SparkContext].toProvider[SparkContextProvider].in[Singleton]
    }

    bind[SQLContext].toProvider[SQLContextProvider].in[Singleton]
  }
}
