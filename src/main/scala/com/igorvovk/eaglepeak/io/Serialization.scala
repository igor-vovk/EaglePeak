package com.igorvovk.eaglepeak.io

import java.io.File

import breeze.io.FileStreams
import com.google.inject.{Inject, Singleton}
import com.twitter.chill.KryoPool

import scala.reflect.ClassTag

@Singleton
class Serialization @Inject()(pool: KryoPool) {

  def save[T: ClassTag](obj: T, path: String) = {
    val file = new File(path)
    if (file.exists()) {
      file.delete()
    } else {
      file.getParentFile.mkdirs()
    }

    file.createNewFile()

    val os = FileStreams.output(file)
    val kryo = pool.borrow()

    try {
      kryo.writeClassAndObject(obj)
      kryo.writeOutputTo(os)
    } finally {
      pool.release(kryo)
      os.close()
    }
  }

  def load[T: ClassTag](path: String): T = {
    val is = FileStreams.input(new File(path))
    val kryo = pool.borrow()

    try {
      kryo.setInput(is)
      val data = kryo.readClassAndObject()

      data.asInstanceOf[T]
    } finally {
      pool.release(kryo)
      is.close()
    }
  }

}
