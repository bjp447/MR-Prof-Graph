import java.io.IOException

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger

/*
* static final TAG_BLOCK
 */
object XMLInputFormat { val TAG_BLOCKS = "xmlProcess.tags.blockTags" }

/*
* Returns a xmlInputReader that is responsible for splitting
* a file into blocks.
 */
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

  /*
  * Splits a file into blocks based on a set of tags specified through
  * a config file.
   */
  class XMLInputReader(split: FileSplit, config: Configuration)
    extends RecordReader[LongWritable, Text]
  {
    private val logger = Logger.getLogger(classOf[XMLInputReader])

    //arrays determining the tags to look for
    private val startTags: Array[Array[Byte]] = initStartTags()
    private val endTags: Array[Array[Byte]] = initEndTags()

    //start and end point of a file split
    private val start: Long = split.getStart
    private val end: Long = start + split.getLength

    //the file passed in during object instantiation
    private val fileIn = split.getPath.getFileSystem(config).open(split.getPath)
    //buffer to store a block of xml before data is copied to a Text
    private val buffer = new DataOutputBuffer()

    //the current key value pair resulting from reading through a file split.
    //keys represent to point at which the block of code was found in a file split
    private var currKey = new LongWritable()
    //values represent the block of code
    private var currValue = new Text()

    /*
    * Create the array of start tags from a config file is possible.
     */
    private def initStartTags(): Array[Array[Byte]] =
    {
      val starttags = config.getStrings(XMLInputFormat.TAG_BLOCKS)
      if (starttags != null) {
        logger.info("config file read for start tags.")
        //map an array of strings into an array of array bytes
        starttags.map(tag => ("<"+ tag).getBytes("UTF-8"))
      }
      else {
        logger.info("config file not read for start tags. using defaults")
        Array("<article".getBytes("UTF-8"),
          "<inproceedings".getBytes("UTF-8"),
          "<incollection".getBytes("UTF-8"),
          "<phdthesis".getBytes("UTF-8"),
          "<mastersthesis".getBytes("UTF-8"))
      }
    }

    /*
    * Create the array of end tags from a config file is possible.
     */
    private def initEndTags(): Array[Array[Byte]] =
    {
      val endtags = config.getStrings(XMLInputFormat.TAG_BLOCKS)
      if (endtags != null) {
        logger.info("config file read for end tags.")
        //map an array of strings into an array of array bytes
        endtags.map(tag => ("</" + tag + ">").getBytes("UTF-8"))
      }
      else {
        logger.info("config file not read for end tags. using defaults")
        Array("</article>".getBytes("UTF-8"),
          "</inproceedings>".getBytes("UTF-8"),
          "</incollection>".getBytes("UTF-8"),
          "</phdthesis>".getBytes("UTF-8"),
          "</mastersthesis>".getBytes("UTF-8"))
      }
    }

    /*
    * Init file split.
     */
    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit =
    {
      this.fileIn.seek(this.start)
    }

    /*
    * Returns true if the fileBuffer starts with the specified tag.
    * Does not advance stream position.
     */
    private def startsWith(fileBuf: FSDataInputStream, tag: Array[Byte]): Boolean =
    {
      var matches = 0 //counter for matching letters to the tag and read in bytes
      val start = fileBuf.getPos //temp value to reset fileBuf pointer location to original

      while (true) {
        val letter = fileBuf.read()
        //false if we reach end of file or passed this splits end point
        if (letter == -1 || (matches == 0 && fileBuf.getPos >= this.end))
        {
          fileBuf.seek(start) //reset stream pointer
          return false
        }

        //found matching letter
        if (letter == tag(matches)) {
          matches += 1
          //return if the entire tag was found
          if (matches >= tag.length) {
            fileBuf.seek(start) //reset stream pointer
            return true
          }
        }
        else { //letter not matched
          fileBuf.seek(start) //reset stream pointer
          return false
        }
      }
      fileBuf.seek(start) //reset stream pointer
      false
    }

    /*
    * Writes to buffer until an ending tag is found.
    * Advances fileBufs position.
     */
    private def writeUntil(fileBuf: FSDataInputStream, endTag: Array[Byte], buffer: DataOutputBuffer): Boolean =
    {
      var matches = 0
      while (true)
      {
        val b = fileBuf.read()
        //return if end of file
        if (b == -1) return false

        buffer.write(b) //store the letter

        //match found
        if (b == endTag(matches))
        {
          matches += 1
          //return if the entire tag was matched
          if (matches >= endTag.length) return true
        }
        else matches = 0 //reset counter to continue searching
      }
      false
    }

    /*
    * Finds the next key value pair
     */
    private def next(key: LongWritable, value: Text): Boolean = {
      try {
        //try to get the next key value pairs until we reach the end of the split
        while (true) {
          //return if file position is passed the end of the split
          if (this.fileIn.getPos >= this.end) return false

          //zip the start and end tags together for searching
          //try every tag for each stream position in the file
          for ((startTag, endTag) <- this.startTags.zip(this.endTags))
          {
            //get value if found a block between two arbitrary tags
            //continues if the file at its current position starts with a tag
            if (startsWith(this.fileIn, startTag))
            {
              //writes into the buffer from start tag till end tag
              if (writeUntil(this.fileIn, endTag, this.buffer))
              {
                key.set(this.fileIn.getPos)
                value.set(this.buffer.getData, 0, this.buffer.getLength)
                logger.info("sending next key value pair with key " + key.get() + " with split progress at " + this.getProgress)
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
        this.buffer.reset() //clear the buffer after every new key value pair or split finish
      }
      false
    }

    /*
    * Get the next pair from the .xml file(s).
     */
    override def nextKeyValue(): Boolean =
    {
      this.currKey = new LongWritable()
      this.currValue = new Text()
      //get the next key value
      //sets these vars and returns whether a next block was found or not
      next(this.currKey, this.currValue)
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



