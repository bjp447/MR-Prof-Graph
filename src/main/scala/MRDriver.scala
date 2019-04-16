import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, MultipleOutputs, TextOutputFormat}
import org.apache.hadoop.io.{ArrayWritable, Text}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger

object MRDriver
{
  private val logger = Logger.getLogger(MRDriver.getClass)
  private var job: Job = _

  private def merge(srcD: String, destF: String, deleteSrc: Boolean): Unit =
  {
    val hdfs = FileSystem.get(job.getConfiguration)
    val srcDir = new Path(srcD)
    val destfile = new Path(destF)

    if (!hdfs.exists(srcDir))
    {
      this.logger.warn("Path: " + srcD + " does not exist!")
      return
    }
    if (hdfs.exists(destfile))
    {
      hdfs.delete(destfile, true)
    }

    try {
      FileUtil.copyMerge(hdfs, srcDir, hdfs, destfile, deleteSrc, job.getConfiguration, null)
    }
    catch {
      case e: Exception => logger.warn("merge failed")
    }
//    hdfs.close()
  }

  private def setFormatPaths(in: String, out: String): Unit =
  {
    val outputPath = new Path(out)

    FileInputFormat.setInputPaths(job, new Path(in))
    FileOutputFormat.setOutputPath(job, outputPath)

    val hdfs = FileSystem.get(job.getConfiguration)
    if (hdfs.exists(outputPath))
    {
      hdfs.delete(outputPath, true)
    }
//    hdfs.close()
  }

  def main(args: Array[String]): Unit =
  {
    val conf = new Configuration()

    //add custom config file information
    conf.addResource(MRDriver.getClass.getClassLoader.getResource("xmlConf.xml"))

    //set separator to ","
    conf.set(TextOutputFormat.SEPERATOR, ",")
    logger.info("text seperator set to: " + conf.get(TextOutputFormat.SEPERATOR))

    //set timeout time in milliseconds
    val time = conf.getInt("xmlProcess.task.timeoutTime", 600000)
    conf.setInt("mapreduce.task.timeout", time)
    logger.info("timeout set to: " + conf.get("mapreduce.task.timeout"))

    //log4j default config
//    BasicConfigurator.configure()

    //job configuration
    this.job = Job.getInstance(conf)

    //get job
    job.setJobName("uic.cs_profs")

    job.setJarByClass(MRDriver.getClass)

    //map output <k,v> classes
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    //reducer output <k,v> classes
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[ArrayWritable])

    //map, combine, reducer classes
    job.setMapperClass(classOf[authorMapper])
    //job.setCombinerClass(classOf[xmlReducer])
    job.setReducerClass(classOf[authorReducer])

    //zoutput file names for nodes and edges
    MultipleOutputs.addNamedOutput(job, "nodedef",
      classOf[TextOutputFormat[Text, Text]], classOf[Text], classOf[Text])
    MultipleOutputs.addNamedOutput(job, "edgedef",
      classOf[TextOutputFormat[Text, Text]], classOf[Text], classOf[Text])

    //input format
    job.setInputFormatClass(classOf[XMLInputFormat])

    println("-------------------"+ args.length +"-------------------")

    //set input and output paths
    if (args.length < 3)
    {
      setFormatPaths("input", "output")
    }
    else
    {
      println(args(0)); println(args(1)); println(args(2));
      setFormatPaths(args(1), args(2))
    }

    //run job and merge node and edge files into one
    if (job.waitForCompletion(true))
    {
      val hdfs = FileSystem.get(job.getConfiguration)

      //create header files for nodes and edge files
      //to be merged with reducer output
      //using GDF format to define graphs
      //create after because outputPath needs to not exist
      //create nodes header
      val out = hdfs.create(new Path(args(2) + "/nodes/anode"))
      out.write("nodedef>name VARCHAR,label VARCHAR, weight DOUBLE\n".getBytes())
      out.close()

      //create edges header
      val out2 = hdfs.create(new Path(args(2) + "/edges/aedge"))
      out2.write("edgedef>node1 VARCHAR,node2 VARCHAR, weight DOUBLE\n".getBytes())
      out2.close()

      //merge files
      merge(args(2) + "/nodes", args(2) + "/nes/anodes", false) //merge nodes into anodes
      merge(args(2) + "/edges", args(2) + "/nes/bedges", false) //merge edges into bedges
      merge(args(2) + "/nes", args(2) + "/prof_graph.gdf", true) //merge nodes and edges into prof_graph.gdf

      hdfs.close()
    }
  }

}
