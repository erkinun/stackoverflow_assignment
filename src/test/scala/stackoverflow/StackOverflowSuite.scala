package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("isQuestion shall return true when type id is 1") {
    val question = Posting(1, 100, None, None, 100, None)

    assert(StackOverflow.isQuestion(question))
  }

  test("isQuestion shall return true when type id is 2") {
    val question = Posting(2, 100, None, Some(1), 100, None)

    assert(!StackOverflow.isQuestion(question))
  }

  test("groupedPostings shall group a question and an answer") {

    val question = Posting(1, 100, None, None, 100, None)
    val answer = Posting(2, 101, None, Some(100), 50, None)

    val rdd = StackOverflow.sc.parallelize(List(question, answer))

    val result = testObject.groupedPostings(rdd).collect().toList

    val expected = List((100, List((question, answer))))

    assert(result.equals(expected))
  }

  test("groupedPostings shall group a question and two answers") {

    val question = Posting(1, 100, None, None, 100, None)
    val answer = Posting(2, 101, None, Some(100), 50, None)
    val answer2 = Posting(2, 102, None, Some(100), 50, None)

    val rdd = StackOverflow.sc.parallelize(List(question, answer, answer2))

    val result = testObject.groupedPostings(rdd).collect().toList

    val expected = List((100, List((question, answer), (question, answer2))))

    assert(result.equals(expected))
  }

}
