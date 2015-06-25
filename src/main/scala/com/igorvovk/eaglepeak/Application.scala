package com.igorvovk.eaglepeak

import com.google.inject.Guice
import com.igorvovk.eaglepeak.guice.{ConfigModule, SparkModule}
import net.codingwell.scalaguice.InjectorExtensions._
import org.apache.spark.SparkContext

object Application extends App {

  println("Hello from Eagle Peek!")

  val injector = Guice.createInjector(
    new ConfigModule,
    new SparkModule()
  )

  val sc = injector.instance[SparkContext]

  //...

  sys.addShutdownHook {
    sc.stop()
  }

}
