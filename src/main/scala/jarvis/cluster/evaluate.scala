//Author: Stephen Pryor
//Feb 24, 2013
//

package jarvis.cluster

import jarvis.math._

object evaluate {
  def main(args: Array[String]) {
    val dataset = readFile(args(0))
    val model = new Kmeans(dataset, useRandomSeed = true)
    val (dispersion, centroids) = model.run(3)
    centroids.foreach(println)
  }

  def readFile(filename: String) = {
    io.Source.fromFile(filename).getLines.map(extractPoint).toIndexedSeq
  }

  def extractPoint(line:String) = {
    line.trim.split("\\s+") match { case Array(i, c, x, y) => Point(IndexedSeq(x.toDouble, y.toDouble)) }
  }
}
