package appliednlp.app

import org.apache.log4j.Level
import org.apache.log4j.Logger
import nak.cluster.Point
import nak.cluster.ClusterConfusionMatrix
import nak.cluster.ClusterReport
import nak.cluster.Kmeans
import nak.cluster.DistanceFunction
import nak.cluster.PointTransformer

import appliednlp.cluster._

import jarvis.cluster.{Kmeans => JarvisKmeans}


/**
 * A standalone object with a main method for reading an input file and running
 * k-means with different options.
 */
object Cluster {

  def main(args: Array[String]) {
    // Parse and get the command-line options
    val opts = ClusterOpts(args)
    
    // This tells the k-means algorithm how much debugging information to show you
    // while you run the algorithm. 
    val logLevel = if (opts.verbose()) Level.DEBUG else Level.INFO
    Logger.getRootLogger.setLevel(logLevel)
    
    // Your code starts here. You'll use and extend it during every problem.
    val data = opts.features() match {
      case "schools" => SchoolsCreator(opts.filename()).toIndexedSeq
      case "countries" => CountriesCreator(opts.filename()).toIndexedSeq
      case "fed-simple" => (new FederalistCreator())(opts.filename()).toIndexedSeq
      case "fed-full" => (new FederalistCreator(true))(opts.filename()).toIndexedSeq
      case _ => DirectCreator(opts.filename()).toIndexedSeq
    }
    val points = data.map(_._3)
    val goldClusterIds = data.map(_._2)
    val model = new Kmeans(PointTransformer(opts.transform(), points)(points), DistanceFunction(opts.distance()), fixedSeedForRandom = true)
    val results = model.run(opts.k())
    val memberships = model.computeClusterMemberships(results._2) 
    val predictedClusterIndices = memberships._2
    val confusion = ClusterConfusionMatrix(goldClusterIds, opts.k(), predictedClusterIndices)
    if(opts.showCentroids()) results._2.foreach(println)
    println(confusion)
    if(opts.report()) ClusterReport(data.map(_._1).toSeq, goldClusterIds, predictedClusterIndices)
  }

}


/**
 * An object that sets of the configuration for command-line options using
 * Scallop and returns the options, ready for use.
 */
object ClusterOpts {

  import org.rogach.scallop._
  
  def apply(args: Array[String]) = new ScallopConf(args) {
    banner("""
Cluster application using nak.cluster.Kmeans

The distance functions are standard cosine, euclidean, and manhattan.

The transform option is one ident, zscore, and pca.
 - ident: no transformation of the input points
 - zscore: use standard z-scores in each dimension to scale the points
 - pca: scale the points with z-scores, then run PCA and use (as transformed dimensions) only the principle components that explain 95% of the variance

For usage see below:
	     """)

    val featureTypes = Set("standard","schools","countries","fed-simple","fed-full")
    val transformers = Set("i","ident","z","zscore","p","pca")
    val distanceFunctions = Set("c","cosine","e","euclidean","m","manhattan")
    val transform = opt[String]("transform", default=Some("ident"), validate = transformers, descr = "The transformation to use. Possible values: " + transformers.toSeq.sorted.mkString(",") )
    val distance = opt[String]("dist", default=Some("cosine"), validate = distanceFunctions, descr = "The distance function to use. Possible values: " + distanceFunctions.toSeq.sorted.mkString(",") )      
    val features = opt[String]("features", default=Some("standard"), validate = featureTypes, descr = "The type of features to extract. Possible values: " + featureTypes.toSeq.sorted.mkString(",") )
    val k = opt[Int]("num-clusters",short='k', required=true, validate = (0<), descr="The number of clusters to find.")
    val help = opt[Boolean]("help", noshort = true, descr = "Show this message")
    val verbose = opt[Boolean]("verbose")
    val showCentroids = opt[Boolean]("output-centroids",short='c', descr="Show centroids.")
    val report = opt[Boolean]("output-report",short='r', descr="Show full cluster report.")
    val filename = trailArg[String]("filename", descr = "The input filename.")
  }
}
