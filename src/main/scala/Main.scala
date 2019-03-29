import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

case class DataPoint(driverID: String, orderID: String, minute: Int, longitude: Float, latitude: Float) extends Serializable

object Main extends Main {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Top-k_Trajectories")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val interestLongitude = 108.99680f
    val interestLatitude = 34.25000f
    val start = 7*60 + 25
    val end = 7*60 + 26
    val k = 1
    val lines = sc.textFile("data/didi_sample_data")
    val dataPoints = rawDataPoints(lines)
    val trajectories = getTrajectories(dataPoints)
    val filteredTrajectories = filterTrajectoriesByTime(trajectories, start, end)
    val trajectoriesWithDistance = appendMinimumDistance(filteredTrajectories, interestLongitude, interestLatitude)
    val result = sc.parallelize(trajectoriesWithDistance.take(k)).map(x => (x._1, x._3))
    result.saveAsTextFile("data/out/result.txt")
  }
}

class Main extends Serializable {

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

  /** Return trajectories with minimum distance to query point sorted by distance in ascending order **/
  def appendMinimumDistance(trajectory: RDD[(String, Iterable[DataPoint])], queryLong: Float, queryLat: Float) :
    RDD[(String, Double, Iterable[DataPoint])] = {
    trajectory.map(x => (x._1, x._2.map(
      x => euclideanDistance(x.longitude, x.latitude, queryLong, queryLat))
      .reduce((x,y) => List(x,y).min), x._2)).sortBy(_._2)
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(x1: Float, y1: Float, x2: Float, y2: Float): Double = {
    Math.sqrt(Math.pow(x2 - x1,2) +  Math.pow(y2 - y1,2))
  }
}
