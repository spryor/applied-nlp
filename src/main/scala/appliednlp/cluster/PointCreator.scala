package appliednlp.cluster

import nak.cluster._
import nak.util.CollectionUtil._
import chalk.util.SimpleTokenizer

import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 *  Read data and produce data points and their features.
 *
 *  @param filename the name of the file containing the data
 *  @return a triple, the first element of which is a sequence of id's
 *     (unique for each row / data point), the second element is the sequence
 *     of (known) cluster labels, and the third of which is the sequence of
 *     Points to be clustered.
 */
trait PointCreator extends (String => Iterator[(String,String,Point)])

/**
 * Read data in the standard format for use with k-means.
 */
object DirectCreator extends PointCreator {

 def apply(filename: String) = {
   io.Source.fromFile(filename).getLines.map(extractPoint)
 }

 def extractPoint(line:String) = {
   line.trim.split("\\s+") match { 
     case Array(i, c, x, y) => {(i, c, Point(IndexedSeq(x.toDouble, y.toDouble)))}
   } 
 }
}

/**
 * A standalone object with a main method for converting the achieve.dat rows
 * into a format suitable for input to RunKmeans.
 */
object SchoolsCreator extends PointCreator {

  val regex = """([\sa-zA-Z.]+)(-?[0-9.]+)\s+(-?[0-9.]+)\s+(-?[0-9.]+)\s+(-?[0-9.]+)""".r

  def apply(filename: String) = {
    io.Source.fromFile(filename).getLines.flatMap(extractSchool)
  }

  def extractSchool(line:String) = {
   line.trim match { 
     case regex(n, r4, m4, r6, m6) => {
       val name = n.trim.replaceAll("\\s+","_")
       Iterator(
         (name+"_4th", "4", Point(IndexedSeq(r4.toDouble, m4.toDouble))), 
         (name+"_6th", "6", Point(IndexedSeq(r6.toDouble, m6.toDouble)))
        )
      }
    }
  } 
}

/**
 * A standalone object with a main method for converting the birth.dat rows
 * into a format suitable for input to RunKmeans.
 */
object CountriesCreator extends PointCreator {

  val regex = """([\sa-zA-Z.]+)(-?[0-9.]+)\s+(-?[0-9.]+)""".r  

  def apply(filename: String) = {
    io.Source.fromFile(filename).getLines.map(extractCountry)
  }

  def extractCountry(line:String) = {
    line.trim match {
      case regex(c, x, y) => { (c.trim.replaceAll("\\s+","_"), "1", Point(IndexedSeq(x.toDouble, y.toDouble))) }
    }
  }

}

/**
 * A class that converts the raw Federalist
 * papers into rows with a format suitable for input to Cluster. As part of
 * this, it must also perform feature extraction, converting the texts into
 * sets of values for each feature (such as a word count or relative
 * frequency).
 */
class FederalistCreator(simple: Boolean = false) extends PointCreator {

  val simpleWordsToLookFor = IndexedSeq("the","people","which")

  def apply(filename: String) = {
    val rawData = FederalistArticleExtractor(filename)
    val texts = rawData.map(_("text"))
    val featureVectors = if(!simple) extractSimple(texts) else extractFull(texts)
    (0 to rawData.length-1).map(i => (rawData(i)("id"), rawData(i)("author").replaceAll("\\s+","_"), featureVectors(i))).toIterator
  }

  /**
   * Given the text of an article, compute the frequency of "the", "people"
   * and "which" and return a Point per article that has the frequency of
   * "the" as the value of the first dimension, the frequency of "people"
   * for the second, and the frequency of "which" for the third.
   *
   * @param texts A sequence of Strings, each of which is the text extracted
   *              for an article (i.e. the "text" field produced by
   *              FederalistArticleExtractor).
   */
  def extractSimple(texts: IndexedSeq[String]): IndexedSeq[Point] = {
    texts.map(text => {
              val tokens = SimpleTokenizer(text)        
              Point(simpleWordsToLookFor.map(w => tokens.count(_.toLowerCase == w).toDouble))
    })
  }

  /**
   * Given the text of an article, extract features as best you can to try to
   * get good alignment of the produced clusters with the known authors.
   *
   * @param texts A sequence of Strings, each of which is the text extracted
   *              for an article (i.e. the "text" field produced by
   *              FederalistArticleExtractor).
   */
  def extractFull(texts: IndexedSeq[String]): IndexedSeq[Point] = {
    val n = 1
    val ngrams = toNgram(n, texts.mkString(" "))
    val documentFreq = texts.map(toNgram(n, _))

    val numNgrams = ngrams.map(_._2).sum
    val selectedFeatures = ngrams.keys.map(ngram => (ngram, ngrams(ngram)/numNgrams)).toMap.filter(_._2 >= .00008).withDefaultValue(0.0)

    (0 to documentFreq.length - 1).map(i => {
      val documentNgrams = documentFreq(i)
      Point(selectedFeatures.keys.toIndexedSeq.map(ngram => documentNgrams(ngram)))
    })
  }

  def toNgram(n:Int, text:String) = {
    SimpleTokenizer(text.toLowerCase).filter(!_.matches("[!'\"?!$:;&,.()]"))
      .sliding(n)
      .toIndexedSeq
      .map{ 
        case Vector(a) => (a)
        case Vector(a, b) => (a, b)
        case Vector(a, b, c) => (a, b, c)
      }.groupBy(x => x)
      .mapValues(_.length.toDouble)
      .withDefaultValue(0.0)
  }

}

object FederalistArticleExtractor {
  /**
   * A method that takes the raw Federalist papers input and extracts each
   * article into a structured format.
   *
   * @param filename The filename containing the Federalist papers.
   * @return A sequence of Maps (one per article) from attributes (like
   *         "title", "id", and "text") to their values for each article.
   */
  def apply(filename: String): IndexedSeq[Map[String, String]] = {

    // Regex to identify the text portion of a document.
    val JustTextRE = (
      """(?s)\*\*\* START OF THIS PROJECT GUTENBERG.+""" +
      """\*\*\*(.+)\*\*\* END OF THIS PROJECT GUTENBERG""").r

    // Regex to capture different parts of each article.
    val ArticleRE = (
      """(?s)(\d+)\n+""" + // The article number.
      """(.+?)\n+""" + // The title (note non-greedy match).
      """((?:(?:For|From)[^\n]+?)?)\s+""" + // The publication venue (optional).
      """((?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday).+\d\d\d\d\.)?)\n+""" + // The date (optional).
      """((?:MAD|HAM|JAY).+?)\n+""" + // The author(s).
      """(To the [^\n]+)""" + // The addressee.
      """(.+)""" // The text.
      ).r

    val book = io.Source.fromFile(filename).mkString
    val text = JustTextRE.findAllIn(book).matchData.next.group(1)
    val rawArticles = text.split("FEDERALIST.? No. ")

    // Use the regular expression to parse the articles.
    val allArticles = rawArticles.flatMap {
      case ArticleRE(id, title, venue, date, author, addressee, text) =>
        Some(Map("id" -> id.trim,
          "title" -> title.replaceAll("\\n+", " ").trim,
          "venue" -> venue.replaceAll("\\n+", " ").trim,
          "date" -> date.replaceAll("\\n+", " ").trim,
          "author" -> author.replaceAll("\\n+", " ").trim,
          "addressee" -> addressee.trim,
          "text" -> text.trim))

      case _ => None
    }.toIndexedSeq

    // Get rid of article 71, which is a duplicate, and return the rest.
    allArticles.take(70) ++ allArticles.slice(71, allArticles.length)
  }

}
