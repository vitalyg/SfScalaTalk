package talk

import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
import com.twitter.scalding._
import edu.berkeley.cs.avro.marker.AvroRecord
import com.twitter.scalding.avro.PackedAvroSource
import com.twitter.algebird._
import com.twitter.scalding.mathematics.Matrix._
import com.twitter.scalding.typed.TDsl._

object SVSSTalk {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration, new Tool, "talk.SVSSTalk" +: "--local" +: args)
  }
}

case class Article(var name: String, var text: String) extends AvroRecord
case class ArticleWordCount(var name: String, var word: String, var count: Double) extends AvroRecord


class SVSSTalk(args: Args) extends Job(args) {
  val dir = "src/main/data/"

  def average = {
    val articles = PackedAvroSource[Article](dir + "articles.avro")
    articles.map(_.text.length.toDouble).aggregate(Averager).write(TypedTsv[Double](dir + "average.txt"))
  }

  object MomentsAggregator extends MonoidAggregator[Article, Moments, List[Any]] {
    def monoid: Monoid[Moments] = MomentsGroup
    def prepare(input: Article): Moments = Moments(input.text.length.toDouble)
    def present(reduction: Moments): List[Any] = reduction.productIterator.toList
  }

  def moments = {
    val articles = PackedAvroSource[Article](dir + "articles.avro")
    articles.aggregate(MomentsAggregator).write(TypedTsv[List[Any]](dir + "moments.txt"))
  }

  def matrix = {
    val articleWordCounts = PackedAvroSource[ArticleWordCount](dir + "wordCounts.avro")
    val mat = articleWordCounts
      .mapToMatrix(wc => (wc.name, wc.word, wc.count))
      .rowL2Normalize

    (mat.getRow("Audi") * mat.transpose)
      .topElems(100)
      .write(TypedTsv[(String, Double)](dir + "matrix.txt"))
  }

  matrix
}























