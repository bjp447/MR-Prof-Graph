import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.log4j.{BasicConfigurator, Logger}

import scala.xml.XML

class authorMapper extends Mapper[LongWritable, Text, Text, Text]
{
  private val logger = Logger.getLogger(classOf[authorMapper])

//  BasicConfigurator.configure()

  private var profs: Array[String] = initProfs()

  def isUIC_CS_prof(name: String): Boolean =
  {
    val b = profs.contains(name)
    if (b)
    {
      this.logger.info("UIC CS PROF found: " + name + "\n")
    }
    b
  }

  private def initProfs(): Array[String] =
  {
    ("Tanya Y. Berger-Wolf\nDaniel J. Bernstein\nEmanuelle Burton\nCornelia Caragea\n" +
      "Debaleena Chattopadhyay\nBarbara Di Eugenio\nJakob Eriksson\nPiotr J. Gmytrasiewicz\nMark Grechanik\n" +
      "Chris Kanich\nRobert V. Kenyon\nAjay D. Kshemkalyani\nJohn Lillis\nWilliam Mansky\n" +
      "G. Elisabeta Marai\nGeorgeta Elisabeta Marai\nEvan McCarty\nNasim Mobasheri\nNatalie Parde\nNatalie Paige Parde\n" +
      "Iasonas Polakis\nJason Polakis\nShanon M. Reckinger\nScott J. Reckinger\nDale Reed\nLuc Renambot\n" +
      "Anastasios Sidiropoulos\nJon A. Solworth\nBrent Stephens\nXiaorui Sun\nMitchell D. Theys\nPatrick Troy\n" +
      "Balajee Vamanan\nOuri Wolfson\nOuri E. Wolfson\nXingbo Wu\nElena Zheleva\nBrian D. Ziebart\nLenore D. Zuck\n" +
      "John T. Bell\nGonzalo A. Bello\nUgo Buy\nUgo A. Buy\nIsabel F. Cruz\nBhaskar DasGupta\nJoseph Hummel\n" +
      "Andrew E. Johnson\nIan A. Kash\nBing Liu 0001\nPeter C. Nelson\nA. Prasad Sistla\nPrasad Sistla\n" +
      "Robert H. Sloan\nV. N. Venkatakrishnan\nPhilip S. Yu\nXinhua Zhang\nDavid Hayes\nCody Cranch").split("\n")
  }

  //setup prof list
  override def setup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit =
  {
    super.setup(context)

    val prfs : Array[String] = context.getConfiguration.getStrings("xmlProcess.uic_cs_profs")
    if (prfs != null)
    {
      logger.info("config file read for profs.")
      this.profs = prfs
    }
    else
    {
      logger.warn("config file for profs not read. Using default values.")
      this.profs = initProfs()
    }
  }

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, Text]#Context): Unit =
  {
    this.logger.info("-----------------------MAPPER_RECIEVED-----------------------\n" + value.toString)
    this.logger.info("---------------------MAPPER_RECIEVED_END---------------------\n")

    try {
      val uri = this.getClass.getClassLoader.getResource("dblp.dtd").toURI
      val n = ("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<!DOCTYPE dblp SYSTEM " +
        "\"" + uri.toASCIIString + "\">\n<dblp>\n").concat(value.toString.concat("\n</dblp>"))

      logger.info("Progress: " + context.getProgress)

      val block = XML.loadString(n.toString)

      val authors = for {
        p <- block.child
        a <- p.child
        if a.label.equals("author") && isUIC_CS_prof(a.text)
      } yield a.text

      //write author connections
      for (author <- authors)
      {
        val auth = new Text(author)
        context.write(auth, new Text("1")) //write that this author wrote 1 publication, authors node weight
        this.logger.info("writing k: " + author + ", v: 1" + "\n")
        for (a <- authors)
        {
          if (author != a)
          {
            context.write(auth, new Text(a)) //connection
            this.logger.info("writing k: " + author + ", v: " + a + "\n")
          }
        }
      }

    }
    catch {
      case e: Exception => this.logger.warn("failed to convert string to XML. may hvae contained unrecognized untity reference.\n" +
        "Skipping block" + "\n", e)
    }

  }

}