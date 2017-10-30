package com.stayrascal.spark.kafka.recommendation

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

object SimilarityReconmender extends Recommender {
  val logger = LoggerFactory.getLogger(getClass)

  def cosineSimilarityInternal(mat: RowMatrix, f: (Double, Double, Double) => Double): CoordinateMatrix = {
    val sums = mat.rows.flatMap { v =>
      v.toSparse.indices.map({ i =>
        i -> math.pow(v(i), 2)
      })
    }.reduceByKey(_ + _)

    val pairs = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> v(i) * v(j)
        }
      }
    }.reduceByKey(_ + _)

    val entries = pairs.map { case ((i, j), v) =>
      i -> (j, v)
    }.join(sums).map { case (i, ((j, v), si)) =>
      j -> (i, v, si)
    }.join(sums).flatMap { case (j, ((i, v, si), sj)) =>
      val value = f(v, si, sj)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }
    new CoordinateMatrix(entries)
  }


  def cosineSimilarity(mat: RowMatrix): CoordinateMatrix = {
    cosineSimilarityInternal(mat, (v, si, sj) => v / math.sqrt(si) / math.sqrt(sj))
  }

  def modifiedCosinesSimilarity(mat: RowMatrix): CoordinateMatrix = {
    cosineSimilarityInternal(mat, (v, si, sj) => v / math.pow(si, 0.6) / math.pow(sj, 0.4))
  }

  def xLogX(x: Long): Double = {
    if (x == 0) 0.0 else x * math.log(x)
  }

  def entropy(elements: Long*): Double = {
    val result = elements.map(xLogX).sum
    val sum = elements.sum
    xLogX(sum) - result
  }

  def logLikelihoodRatio(k11: Long, k12: Long, k21: Long, k22: Long): Double = {
    val rowEntropy = entropy(k11 + k12, k21 + k22)
    val columnEntropy = entropy(k11 + k21, k12 + k22)
    val matrixEntropy = entropy(k11, k12, k21, k22)
    if (rowEntropy + columnEntropy < matrixEntropy) {
      0.0
    } else {
      2.0 * (rowEntropy + columnEntropy - matrixEntropy)
    }
  }

  def llr(mat: RowMatrix): CoordinateMatrix = {
    mat.rows.cache()
    val numRows = mat.numRows

    val pairs = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> 1
        }
      }
    }.reduceByKey(_ + _)

    val items = mat.rows.flatMap { v =>
      v.toSparse.indices.map { i =>
        i -> 1
      }
    }.reduceByKey(_ + _).persist

    val entries = pairs.map { case ((i, j), v) =>
      i -> (j, v)
    }.join(items).map { case (i, ((j, v), si)) =>
      j -> (i, v, si)
    }.join(items).flatMap { case (j, ((i, v, si), sj)) =>
      val k11 = v
      val k12 = sj - v
      val k21 = si - v
      val k22 = numRows - si - sj + v
      val value = logLikelihoodRatio(k11, k12, k21, k22)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }

    new CoordinateMatrix(entries)
  }

  def jaccard(mat: RowMatrix): CoordinateMatrix = {
    mat.rows.persist()
    val pairs = mat.rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> 1
        }
      }
    }.reduceByKey(_ + _)

    val items = mat.rows.flatMap { v =>
      v.toSparse.indices.map { i => i -> 1 }
    }.reduceByKey(_ + _).persist()

    val entries = pairs.map { case ((i, j), v) => i -> (j, v)
    }.join(items).map { case (i, ((j, v), si)) => j -> (i, v, si)
    }.join(items).flatMap { case (j, ((i, v, si), sj)) =>
      val value = v.toDouble / (si + sj - v)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }
    new CoordinateMatrix(entries)
  }

  def normalize(values: Iterable[(Int, Double)]): Iterable[(Int, Double)] = {
    val count = values.size
    val mean = values.map(_._2).sum / count
    val sd = values.map(_._2).map(v => math.pow(v - mean, 2)).sum / count
    val std = math.sqrt(sd)
    values.map(t => t._1 -> (t._2 - mean) / std)
  }

  def euclidean(mat: CoordinateMatrix): CoordinateMatrix = {
    val normEntries = mat.toIndexedRowMatrix().rows.flatMap { row =>
      val values = row.vector.toSparse.indices.map { j =>
        j -> row.vector(j)
      }
      normalize(values).filterNot(_._2.isNaN).map { case (j, value) =>
        MatrixEntry(row.index, k, value)
      }
    }
    val normMat = new CoordinateMatrix(normEntries)
    val pairs = normMat.toRowMatrix().rows.flatMap { v =>
      val indices = v.toSparse.indices
      indices.flatMap { i =>
        indices.filter(_ > i).map { j =>
          (i, j) -> math.pow(v(i) - v(j), 2)
        }
      }
    }.reduceByKey(_ + _)

    val entries = pairs.flatMap { case ((i, j), v) =>
      val value = math.sqrt(v)
      Seq(MatrixEntry(i, j, value), MatrixEntry(j, i, value))
    }
    new CoordinateMatrix(entries)
  }


  def normalizeLp(products: Iterable[(Int, Double)], p: Int): Iterable[(Int, Double)] = {
    val lpnorm = math.pow(products.map(t => math.pow(t._2, p)).sum, 1.0 / p)
    products.map(t => t._1 -> t._2 / lpnorm)
  }

  def normalizeRange(products: Iterable[(Int, Double)]): Iterable[(Int, Double)] = {
    val min = products.map(_._2).min
    val max = products.map(_._2).max
    if (min == max) {
      products.map(t => t._1 -> 0.5)
    } else {
      products.map(t => t._1 -> (t._2 - min) / (max - min))
    }
  }

  override def recommend(trainingSet: RDD[Rating], params: Map[String, Any]): RDD[(Int, Seq[Rating])] = {
    val numNeighbours = params.getInt("numNeighbours")
    val numRecommendations = params.getInt("numRecommendations")
    val similarityMethod = params.getString("similarityMethod")
    val recommendMethod = params.getString("recommendMethod")
    val outputPath = params.getString("outputPath")

    val entries = trainingSet.map({ case Rating(user, product, rating) =>
      MatrixEntry(user, product, rating)
    })

    val coorMat = new CoordinateMatrix(entries)
    val mat = coorMat.toRowMatrix

    val sim = similarityMethod match {
      case "spark" => mat.columnSimilarities()
      case "spark-appr" => mat.columnSimilarities(0.1)
      case "cosine" => cosineSimilarity(mat)
      case "cosine-mod" => modifiedCosinesSimilarity(mat)
      case "llr" => llr(mat)
      case "jaccard" => jaccard(mat)
      case "euclidean" => euclidean(coorMat)
      case _ => throw new IllegalArgumentException("unknown similarity method")
    }

    val simTop = sim.entries.map { case MatrixEntry(i, j, u) =>
      i.toInt -> (j.toInt, u)
    }.groupByKey.mapValues { products =>
      val productsTop = products.toSeq.sortWith(_._2 > _._2).take(numNeighbours)
      normalizeRange(productsTop)
    }.persist()
  }
}
