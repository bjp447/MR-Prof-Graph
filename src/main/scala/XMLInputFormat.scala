import java.io.IOException

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

object XMLInputFormat { val TAG_BLOCK = "xmlProcess.tags.blockTags" }

class XMLInputFormat extends TextInputFormat
{
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[LongWritable, Text] =
  {
    try {
      new XMLInputReader(split.asInstanceOf[FileSplit], context.getConfiguration)
    }
    catch {
      case e: IOException => Logger.getLogger(classOf[XMLInputFormat]).error("error creating recordReader")
        null
    }
  }

  class XMLInputReader(split: FileSplit, config: Configuration)
    extends RecordReader[LongWritable, Text]
  {
    private val logger = Logger.getLogger(classOf[XMLInputReader])

//    BasicConfigurator.configure() //log4j default config

    //arrays determining the tags to look for
    private val startTags: Array[Array[Byte]] = initStartTags()
    private val endTags: Array[Array[Byte]] = initEndTags()

    private val start: Long = split.getStart
    private val end: Long = start + split.getLength

    private val fileIn = split.getPath.getFileSystem(config).open(split.getPath)
    private val buffer = new DataOutputBuffer()

    private var currKey = new LongWritable()
    private var currValue = new Text()

    private def initStartTags(): Array[Array[Byte]] =
    {
      val starttags = config.getStrings(XMLInputFormat.TAG_BLOCK)
      if (starttags != null)
      {
        logger.info("config file read for start tags.")
        for {
          tag <- starttags
          t <- ("<" + tag).getBytes("UTF-8")
        } yield t
      }
      else
      {
        logger.info("config file not read for start tags. using defaults")
      }

      Array("<article".getBytes("UTF-8"),
        "<inproceedings".getBytes("UTF-8"),
        "<incollection".getBytes("UTF-8"),
        "<phdthesis".getBytes("UTF-8"),
        "<mastersthesis".getBytes("UTF-8"));
    }

    private def initEndTags(): Array[Array[Byte]] =
    {
      val endtags = config.getStrings(XMLInputFormat.TAG_BLOCK)
      if (endtags != null)
      {
        logger.info("config file read for end tags.")
        for {
          tag <- endtags
          t <- ("</" + tag + ">").getBytes("UTF-8")
        } yield t
      }
      else
      {
        logger.info("config file not read for end tags. using defaults")
      }

      Array("</article>".getBytes("UTF-8"),
        "</inproceedings>".getBytes("UTF-8"),
        "</incollection>".getBytes("UTF-8"),
        "</phdthesis>".getBytes("UTF-8"),
        "</mastersthesis>".getBytes("UTF-8"));
    }

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
    {
      this.fileIn.seek(this.start)
    }

    //returns true if the buffer starts with tag
    //does not advance stream position
    private def startsWith(fileBuf: FSDataInputStream, tag: Array[Byte]): Boolean =
    {
      var i1 = 0
      val start = fileBuf.getPos

      while (true) {
        val letter = fileBuf.read()
        if (letter == -1 || (i1 == 0 && fileBuf.getPos >= this.end))
        {
          fileBuf.seek(start)
          return false
        }

        if (letter == tag(i1)) {
          i1 += 1
          if (i1 >= tag.length) {
            fileBuf.seek(start)
            return true
          }
        }
        else {
          fileBuf.seek(start)
          return false
        }
      }
      fileBuf.seek(start)
      false
    }

    //writes to buffer until tag is found
    //advances fileBufs position
    private def writeUntil(fileBuf: FSDataInputStream, endTag: Array[Byte], buffer: DataOutputBuffer): Boolean =
    {
      var i = 0
      while (true)
      {
        val b = fileBuf.read()
        if (b == -1) return false

        buffer.write(b)

        if (b == endTag(i))
        {
          i += 1
          if (i >= endTag.length) return true
        }
        else i = 0
//        if (fileBuf.getPos >= this.end) return false
      }
      false
    }

    //finds the next key value pair
    private def next(key: LongWritable, value: Text): Boolean =
    {
      try {
        while (true)
        {
          if (this.fileIn.getPos >= this.end) return false

          for ((tag, index) <- this.startTags.zipWithIndex)
          {
            //get value if found a block between two arbitrary tags
            if (startsWith(this.fileIn, tag))
            {
              //writes from start tag till end tag
              if (writeUntil(this.fileIn, this.endTags(index), this.buffer))
              {
                key.set(this.fileIn.getPos)
                value.set(this.buffer.getData, 0, this.buffer.getLength)
//                logger.info("sending next key value pair with key " + key.get() + " with split progress at " + this.getProgress)
                return true
              }
            }
          }
          this.fileIn.read() //advance stream position
        }
      }
      catch {
        case e: IOException => this.logger.error("error with next", e)
      }
      finally {
        this.buffer.reset()
      }
      false
    }

    //get the next pair from the .xml file(s)
    override def nextKeyValue(): Boolean =
    {
      this.currKey = new LongWritable()
      this.currValue = new Text()
      val b = next(this.currKey, this.currValue)
      b
    }

    override def close(): Unit = fileIn.close()

    override def getProgress: Float = {
      (this.fileIn.getPos - this.start) /
        (this.end - this.start).asInstanceOf[Float]
    }

    override def getCurrentKey: LongWritable = this.currKey
    override def getCurrentValue: Text = this.currValue
  }
}



