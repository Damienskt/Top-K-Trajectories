import java.io.PrintWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag
import java.text.SimpleDateFormat

import com.vividsolutions.jts.geom._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{CircleRDD, LineStringRDD, PointRDD, SpatialRDD}
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery}


case class DataPoint(driverID: String, orderID: String, minute: Int, longitude: Float, latitude: Float) extends Serializable

object KNN extends KNN {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Top-k_Trajectories")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val querypt = sc.textFile("data/input/query")
    val test = querypt.map(line => {
      val arr = line.split(",")
      (arr(0).toFloat, arr(1).toFloat,arr(2).toInt,arr(3).toInt,arr(4).toInt,arr(5).toInt,arr(6).toInt)
    })
    val k = test.collect().apply(0)._3
    val startHr = test.collect().apply(0)._4
    val startMin = test.collect().apply(0)._5
    val endHr = test.collect().apply(0)._6
    val endMin = test.collect().apply(0)._7

    val start = startHr * 60 + startMin
    val end = endHr * 60 + endMin
    val count = 0
    val lines = sc.textFile("data/input/didi_sample_data")
    val dataPoints = rawDataPoints(lines)
    //dataPoints.saveAsTextFile("data/out/test.txt")
    val trajectories = getTrajectories(dataPoints)
    val filteredTrajectories = filterTrajectoriesByTime(trajectories, start, end)

    val fact = new GeometryFactory()
    //filteredTrajectories.map(x => x._2)
    val javardd = filteredTrajectories.map(x => fact.createLineString(x._2.map(y => new Coordinate(y.longitude, y.latitude)).toArray)).toJavaRDD()

      //.map(y => fact.createPoint(new Coordinate(y.longitude.toFloat, y.latitude.toFloat))).toJavaRDD()
    //javardd.saveAsTextFile("data/output/test.txt")
    val trajectPoints = new LineStringRDD()
    trajectPoints.setRawSpatialRDD(javardd)
    //trajectPoints.saveAsGeoJSON("data/output/test")

    //val query = test.map(x => fact.createPoint(new Coordinate(x._1.toFloat,x._2.toFloat))).toJavaRDD()
    val query = test.map(x => fact.createPoint(new Coordinate(x._1.toFloat,x._2.toFloat)))
    val longitude = query.collect().apply(0).getX
    val latitude = query.collect().apply(0).getY

//    val circlePoints = new PointRDD()
//    circlePoints.setRawSpatialRDD(query)
//
//    val circleRdd = new CircleRDD(circlePoints, 0.1)
//    val pointRDD = new PointRDD()

    trajectPoints.analyze()
////    //circleRdd.analyze()
    //trajectPoints.spatialPartitioning(GridType.RTREE)
//    circleRdd.spatialPartitioning(trajectPoints.getPartitioner)

    //trajectPoints.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    //circleRdd.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    trajectPoints.buildIndex(IndexType.RTREE,false)

    //trajectPoints.saveAsGeoJSON("data/output/traject")
    //circleRdd.saveAsGeoJSON("data/output/circle")
    println(longitude +","+latitude)
    val result = KNNQuery.SpatialKnnQuery(trajectPoints,fact.createPoint(new Coordinate(longitude,latitude)),k,true)
    result.forEach(x => sc.parallelize(x.getCoordinates.map(input => input.x + ","+ input.y)).saveAsTextFile("data/output/result"+{var i = 0; () => { i += 1; i}}))
//    sc.parallelize(result.get(0).getCoordinates.map(x => x.x + "," +x.y)).saveAsTextFile("data/output/result1")
//    sc.parallelize(result.get(1).getCoordinates.map(x => x.x + "," +x.y)).saveAsTextFile("data/output/result2")
//    sc.parallelize(result.get(2).getCoordinates.map(x => x.x + "," +x.y)).saveAsTextFile("data/output/result3")
//    sc.parallelize(result.get(3).getCoordinates.map(x => x.x + "," +x.y)).saveAsTextFile("data/output/result4")


    //val result = JoinQuery.DistanceJoinQueryFlat(trajectPoints,circleRdd,false,true).count()
  }

}

class KNN extends Serializable {

  final val MAX_MINUTE = 1439

  /** Load data points from the given file */
  def rawDataPoints(lines: RDD[String]): RDD[DataPoint] = {
    val hourDf:SimpleDateFormat = new SimpleDateFormat("HH")
    val minuteDf:SimpleDateFormat = new SimpleDateFormat("mm")
    lines.map(line => {
      val arr = line.split(",")
      DataPoint(
        driverID = arr(0),
        orderID = arr(1),
        minute = (hourDf.format(arr(2).toLong * 1000L).toInt * 60) + minuteDf.format(arr(2).toLong * 1000L).toInt,
        longitude = arr(3).toFloat,
        latitude = arr(4).toFloat)
    })
  }

  /** Group the trajectories together */
  def getTrajectories(dataPoints: RDD[DataPoint]): RDD[(String, Iterable[DataPoint])] = {
    dataPoints.map(dp => (dp.orderID, dp)).groupByKey()
  }

  /** Filter the trajectories based on user input time */
  def filterTrajectoriesByTime(dataPoints: RDD[(String, Iterable[DataPoint])], start: Int, end: Int):
  RDD[(String, Iterable[DataPoint])] = {
    dataPoints.filter { case (key, xs) =>
      xs.exists { xss =>
        if (start <= end) {
          (xss.minute >= start) && (xss.minute <= end)
        } else {
          (xss.minute >= start) && (xss.minute <= MAX_MINUTE) || (xss.minute >= 0) && (xss.minute <= end)
        }
      }
    }
  }
}
