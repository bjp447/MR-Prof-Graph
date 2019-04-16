Brennan Pohl, 2/27/2019

Oringinaly written as a homework assignment for a Cloud Computing course, Spring 2019.

Using the mapreduce model provided in Apache Hadoop, and publication data from https://dblp.uni-trier.de/ this project produces a graph file that maps authors to co-authors.

The graph file serves as input to Gephi: https://gephi.org/.
The graph file format is GDF. https://gephi.org/users/supported-graph-formats/gdf-format/

The authors that are considered are UIC CS professors by default but can be configured in the file 'xmlConf.xml' in /src/main/resources.


When assembling a fresh jar they show up in '/target/scalla-2.11/'.

A picture and the gdf file of the resulting map-reduce run with a the first 150mb of the data is included.

To create the jar that will be used to execute the program:
In the main directory execute the following command. It will take a few minutes. 
This command will also run the tests.
>sbt assembly

This will create a very fat jar package called 'hw2.jar'

To run the program:

* First create the input directory and place the file to be processed into hdfs
> hdfs dfs -mkdir input

> hdfs dfs -put [path to dblp.xml] input

* Then run it via hadoop jar [path to jar] [main class name] [input folder path] [output folder path]
> hadoop jar hw2.jar hw2 input output

The input folder should have the data to be processed.
The output folder does not have to exist. The program will either create the folder path or overwrite the contents of the existing path

The output of the program will be called 'prof_graph.gdf' and will be inside the hdfs.
* To retrieve the file and place into the current directory:
> hdfs dfs -get output/prof_graph.gdf


The file is built for visualizing with Gephi. 
To display the graph:
In the Welcome page on start up: 
> Open Graph File

or

> file -> open -> open... -> choose 'prof_graph.gdf'

Make sure undirected is chosen and 'edges merge stratrgy' is on 'Sum' and click ok. 

To get the desired view for the nodes, click the size icon under Appearance, ranking and choose weight and apply.


A config file is used to define the xml tags to use when splitting an xml file, the UIC CS Professor list, and the task timeout time. 'xmlConf.xml'
To overwrite the file in the jar: 
> jar uf hw2.jar xmlConf.xml

//------------------------------------------
The config property's look somethinkg like:

<property>
<name>xmlProcess.tags.blockTags</name>
<value>article,inproceedings,incollection,phdthesis,mastersthesis</value>
</property>
<property>
<name>xmlProcess.uic_cs_profs</name>
<value>Tanya Y. Berger-Wolf,Daniel J. Bernstein,Emanuelle Burton,Cornelia Caragea</value>
<property>
<name>xmlProcess.task.timeoutTime</name>
<value>600000</value>
</property>

//------------------------------------------

//------------------------------------------

The .gdf graphing file will look something like this:

nodedef>name VARCHAR,label VARCHAR, label, weight DOUBLE
Daniel J. Bernstein,Daniel J. Bernstein,true,15
edgedef>node1 VARCHAR,node2 VARCHAR, weight DOUBLE
Daniel J. Bernstein,Tanya Y. Berger-Wolf,1

//------------------------------------------

The mapper takes in <IntWritable, Text> and outputs <Text, Text>.

The input's key is the position of the block in the .xml file(s).
The input's value is an xml block that may is contained between several different tags. Such as <article>...</article> or <inproceedings>...</inproceedings>

The output's key is a UIC CS Professor, and it's values are the corresponding authors coauthors and a '1' for each author to indicate a publication by said author.

The reducer takes in an author and a collection of coauthors, in which the reducer counts the amount of '1's to determine the amount of publications the author authored. The represents the weight of a node.
The reducer outputs the author key and the amount of articles written to a nodes folder.
The reducer outputs the author's coauthors to another file location for edges.

Then the files in these folder are merged with their respective header files that defines the graph format and then these resulting files are merged into a file named 'prof_graph.gdf' placed into the output folder path specified when the job was launched.
