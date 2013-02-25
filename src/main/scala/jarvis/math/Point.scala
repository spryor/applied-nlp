//Author: Stephen Pryor
//Feb 24, 2013

package jarvis.math

case class Point(val vector: IndexedSeq[Double]){
  import math.sqrt
  
  def zip(other: Point) = this.vector.zip(other.vector)

  def +(other: Point) = Point(this.zip(other).map{ case (a, b) => a + b })

  def -(other: Point) = Point(this.zip(other).map{ case (a, b) => a - b })
  
  def /(divisor: Double) = Point(vector.map(_ / divisor))

  def dotProduct(other: Point) = this.zip(other).map{ case (a, b) => a * b }.sum

  lazy val norm = sqrt(this.dotProduct(this))

  lazy val abs = Point(vector.map(_.abs))

  lazy val dim = vector.length

  lazy val sum = vector.sum

  lazy val product = vector.product

  override def toString = "[" + vector.mkString(",") + "]"
}

trait DistanceFunction extends ((Point, Point) => Double)

object DistanceFunction {
  def apply(choice: String) = choice match {
    case "e" | "euclidean" => EuclideanDistance
    case _ => throw new MatchError("Unknown distance function: " + choice)
  }
}

object EuclideanDistance extends DistanceFunction {
  def apply(x: Point, y: Point) = (x - y).norm
}
