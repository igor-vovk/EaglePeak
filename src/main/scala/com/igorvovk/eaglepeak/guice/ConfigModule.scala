package com.igorvovk.eaglepeak.guice

import com.google.inject.{AbstractModule, Provider}
import com.typesafe.config.{Config, ConfigFactory}
import net.codingwell.scalaguice.ScalaModule


object ConfigModule {

  class ConfigProvider extends Provider[Config] {
    override def get(): Config = ConfigFactory.load()
  }

}

/**
 * Binds the application configuration to the [[com.typesafe.config.Config]] interface.
 *
 * The config is bound as an eager singleton so that errors in the config are detected
 * as early as possible.
 */
class ConfigModule extends AbstractModule with ScalaModule {
  import ConfigModule._

  override def configure(): Unit = {
    bind[Config].toProvider[ConfigProvider].asEagerSingleton()
  }
}