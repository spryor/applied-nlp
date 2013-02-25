//Author: Stephen Pryor
//Feb 24, 2013
//

package jarvis.cluster

import jarvis.math._

class Kmeans(
      points: IndexedSeq[Point],
      distFunc: DistanceFunction = DistanceFunction("e"),
      minChangeInDispersion: Double = 0.0001,
      maxIter: Int = 200,
      useRandomSeed: Boolean = true
      ){

  import scala.util.Random

  lazy private val random = new Random()
  
  def run(k: Int, attempts: Int = {if(useRandomSeed) 20 else 1}): (Double,  IndexedSeq[Point]) = {
    (1 to attempts).map{ _ => 
      moveCentroids(chooseRandomCentroids(k))
    }.minBy(_._1)
  }

  def moveCentroids(centroids: IndexedSeq[Point]): (Double,  IndexedSeq[Point]) = {
    def step(centroids: IndexedSeq[Point], 
             prevDispersion: Double, 
             iter: Int): (Double, IndexedSeq[Point]) = {
      if(iter > maxIter) {
        (prevDispersion, centroids)
      } else {
        val (dispersion, assignments) = assignClusters(centroids)

        if((prevDispersion - dispersion) < minChangeInDispersion)
          (prevDispersion, centroids)
        else
          step(updateCentroids(assignments), dispersion, iter + 1)
      }
    }
    step(centroids, Double.PositiveInfinity, 1)
  }

  def assignClusters(centroids: IndexedSeq[Point]) = {
    val (squaredDistances, assignments) = points.map(assignPoint(_,centroids)).unzip
    (squaredDistances.sum, assignments)
  }

  private[this] def assignPoint(point: Point, centroids: IndexedSeq[Point]) = {
    var shortestDist = distFunc(centroids(0),point)
    var assignment = 0
    var i = 1
    while(i < centroids.length){
      val currentDistance = distFunc(centroids(i),point)
      if(currentDistance < shortestDist) {
        shortestDist = currentDistance
        assignment = i
      }
      i += 1
    }
    (shortestDist * shortestDist, assignment)
  }

  private def updateCentroids(assignments: IndexedSeq[Int]) = {
    assignments
      .zip(points)
      .groupBy(k => k._1) //group by cluster assignment
      .mapValues(cluster => cluster.map(_._2).reduce(_ + _) / cluster.length.toDouble)
      .map(_._2)
      .toIndexedSeq
  }

  private def chooseRandomCentroids(k: Int) = {
    if(!useRandomSeed) random.setSeed(13)
    random.shuffle(points).take(k)
  }


  private def chooseKmeansPlusPlusCentroids(k: Int) = {
    if(!useRandomSeed) random.setSeed(13)
    val firstCentroid = random.shuffle(points).take(1)
    
    def stepKmeansPlusPlus(centroids: IndexedSeq[Point], k: Int): IndexedSeq[Point] = {
      centroids
    }

    stepKmeansPlusPlus(firstCentroid, k)
  }
}
