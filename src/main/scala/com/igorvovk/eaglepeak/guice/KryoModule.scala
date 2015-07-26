package com.igorvovk.eaglepeak.guice

import com.google.inject.{AbstractModule, Provider, Singleton}
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import net.codingwell.scalaguice.ScalaModule


@Singleton
class KryoPoolProvider extends Provider[KryoPool] {
  override def get() = ScalaKryoInstantiator.defaultPool
}

class KryoModule extends AbstractModule with ScalaModule {
  override def configure(): Unit = {
    bind[KryoPool].toProvider[KryoPoolProvider].asEagerSingleton()
  }
}
