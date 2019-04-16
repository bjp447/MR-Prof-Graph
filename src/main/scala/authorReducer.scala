import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.Iterable

class authorReducer extends Reducer[Text, Text, Text, Text]
{
  private val logger = Logger.getLogger(classOf[authorReducer])
  private var out: MultipleOutputs[Text, Text] = _

  def toInt(s: Text): Int =
  {
    try {
      s.toString.toInt
    } catch {
      case e: Exception => 0
    }
  }

  def toArr(values : Iterable[Text]): Array[Text] =
  {
    var cache = new ArrayBuffer[Text]()

    for (name <- values)
    {
      cache += new Text(name)
    }
    cache.toArray
  }

  override def setup(context: Reducer[Text, Text, Text, Text]#Context): Unit =
  {
    this.out = new MultipleOutputs[Text, Text](context)
//    BasicConfigurator.configure()
  }

  override def reduce(key: Text, values: lang.Iterable[Text],
                      context: Reducer[Text, Text, Text, Text]#Context): Unit =
  {
    this.logger.info("-----------------------REDUCER_RECIEVED-----------------------\n" + key.toString)

    val cache = toArr(values.asScala) //cache the iterable

    for (name <- cache) //logging
    {
      this.logger.info(name.toString)
    }

    val names: Array[Text] = cache.filter(toInt(_) == 0) //filter out '1's
    val articles: Array[Text] = cache.filter(toInt(_) > 0) //filter out names

    //logging
    this.logger.info("articles authos published: " + articles.length + "\n" )
    this.logger.info("coAuthors " + names.length + ": " + "\n")
    for (name <- names)
    {
      this.logger.info(name.toString + "\n" )
    }
    this.logger.info("---------------------REDUCER_RECIEVED_END---------------------\n")

    //write the node
    //node = {name, label, weight}
    val node = new Text(key.toString + "," + articles.length)
    out.write("nodedef", key, node, "nodes/nodedef")
	//out.write(key, node, "nodes/nodedef")

    logger.info("writing key: " + key.toString + "\n")
    logger.info("Writing values: " + "\n")
    for (name <- names)
    {
      //write the edge
      //edge = {node1, node2, weight}
      out.write("edgedef", key, new Text(name.toString + ",1"), "edges/edgedef")
	  //out.write(key, new Text(name.toString + ",1"), "edges/edgedef")
      this.logger.info(name.toString)
    }
  }

  override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit =
  {
    this.out.close()
  }
}
