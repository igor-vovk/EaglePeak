package com.igorvovk.eaglepeak.service

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import scala.reflect.ClassTag

/**
 * [[http://blog.madhukaraphatak.com/kryo-disk-serialization-in-spark/]]
 */
object IO {

  def saveRDD[T: ClassTag](rdd: RDD[T], path: String) = {
    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

    rdd.mapPartitions(iter => {
      iter.grouped(10).map(_.toArray).map(splitArray => {
        val kryo = kryoSerializer.newKryo()

        val bao = new ByteArrayOutputStream()
        val output = kryoSerializer.newKryoOutput()
        output.setOutputStream(bao)
        kryo.writeClassAndObject(output, splitArray)
        output.close()

        val byteWritable = new BytesWritable(bao.toByteArray)

        (NullWritable.get(), byteWritable)
      })
    }).saveAsSequenceFile(path)
  }

  def loadRDD[T: ClassTag](sc: SparkContext, path: String, minPartitions: Int = 1): RDD[T] = {
    val kryoSerializer = new KryoSerializer(sc.getConf)

    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions).flatMap(x => {
      val kryo = kryoSerializer.newKryo()
      val input = new Input()
      input.setBuffer(x._2.getBytes)
      val data = kryo.readClassAndObject(input)
      val dataObject = data.asInstanceOf[Array[T]]
      dataObject
    })
  }

}
