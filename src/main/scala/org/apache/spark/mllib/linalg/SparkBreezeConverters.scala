package org.apache.spark.mllib.linalg

import org.apache.spark.mllib.linalg.distributed.DistributedMatrix

object SparkBreezeConverters {

  implicit class PimpedSparkVector(val vector: Vector) extends AnyVal {
    def toBreeze: breeze.linalg.Vector[Double] = vector.toBreeze
  }

  implicit class PimpedBreezeVector(val vector: breeze.linalg.Vector[Double]) extends AnyVal {
    def toSpark: Vector = Vectors.fromBreeze(vector)
  }

  implicit class PimpedSparkMatrix(val matrix: Matrix) extends AnyVal {
    def toBreeze: breeze.linalg.Matrix[Double] = matrix.toBreeze
  }

  implicit class PimpedSparkDistributedMatrix(val matrix: DistributedMatrix) extends AnyVal {
    def toBreeze: breeze.linalg.Matrix[Double] = matrix.toBreeze()
  }

  implicit class PimpedBreezeMatrix(val matrix: breeze.linalg.Matrix[Double]) extends AnyVal {
    def toSpark: Matrix = Matrices.fromBreeze(matrix)
  }

}
