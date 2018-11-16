//Kedarnath Kurnool Gandla
//ID: 801006369
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.DoubleWritable;
import java.io.FileNotFoundException;
import java.lang.*;
import java.util.*;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.regex.Matcher;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.conf.Configuration;


public class PageRank extends Configured implements Tool {
   private static final Logger LOG = Logger .getLogger( PageRank.class);
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new PageRank(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      boolean result;
      Configuration conf = getConf();
      Path output1 = new Path(args[1]); // This is the path for the first output
      Path output2 = new Path(args[2]); // This is the path for the second out
      Path tempoutputfile = new Path("outputtemp/part-r-00000");
      Path tempoutput = new Path("outputtemp");
      FileSystem hdfs = FileSystem.get(conf);
      Job job  = Job .getInstance(getConf(), " pagerank ");
      //Below job is to calculate number of total pages in the given corpus
      job.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job,  args[0]); //input file path is taken here
      FileOutputFormat.setOutputPath(job,  new Path("outputtemp"));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);
      job.setNumReduceTasks(1);
      result = job.waitForCompletion( true);
      FileSystem fs = FileSystem.get(getConf());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(tempoutputfile)));
      String[] count;
      String line = "";
      double totalcount=0.0;
      if((line = reader.readLine())!=null){
	   count = line.split("\\s+");
	   totalcount = Double.parseDouble(count[1].trim());
      }
      if (hdfs.exists(tempoutput)) {
   		 	hdfs.delete(tempoutput, true);
       }
      if(result){
	      conf.set("TotalCount", Double.toString(totalcount)); 
	      Job job1  = Job .getInstance(getConf(), " pagerank ");
	      job1.setJarByClass( this .getClass());
	      FileInputFormat.addInputPaths(job1,  args[0]); //input file path is taken here
	      FileOutputFormat.setOutputPath(job1,  new Path(args[1])); // output path is set here.
	      job1.setMapperClass( Map1 .class);
	      job1.setReducerClass( Reduce1 .class);
	      job1.setOutputKeyClass( Text .class);
	      job1.setOutputValueClass( Text .class);
	      job1.setNumReduceTasks(1);
	      result = job1.waitForCompletion( true);
      }
      if(result){
	  //The below for loop runs by taking the input from the previous map reduce output.
	  //Once the new output is generated, previous input directory is being deleted and the same name is used as the output of the current map reduce.
	  //This procedure will be iterated 10 times and finally the previous map reduce output will be the output for this map reduce process.
	  for(int i = 0; i< 10; i++){
	      if(i%2 == 0){
		  //check if the file already exists and deletes if it exists.
	 	  if (hdfs.exists(output2)) {
   		 	hdfs.delete(output2, true);
	  	 }
	      }
	      else{
		  //check if the file already exists and deletes if it exists.
	 	  if (hdfs.exists(output1)) {
   		 	hdfs.delete(output1, true);
	  	 }
	      }
	      Job job2  = Job .getInstance(getConf(), " pagerank ");
	      job2.setJarByClass( this .getClass());
	      //The below if else condition is to feed the input according to the 'i' value
	      if(i%2 == 0){
	          FileInputFormat.addInputPaths(job2,  args[1]); //input file path is taken here
	      	  FileOutputFormat.setOutputPath(job2,  new Path(args[2]));
	      }
	      else{
	          FileInputFormat.addInputPaths(job2,  args[2]); //input file path is taken here
	      	  FileOutputFormat.setOutputPath(job2,  new Path(args[1]));
	      }
	      //output path is set here.
	      job2.setMapperClass( Map2 .class);
	      job2.setReducerClass( Reduce2 .class);
	      job2.setOutputKeyClass( Text .class);
	      job2.setOutputValueClass( Text .class);
	      job2.setNumReduceTasks(1);
	      result = job2.waitForCompletion( true);
	  }
	  //Checks if the newly created output folder is available and deletes if that exists.
 	  if (hdfs.exists(output2)) {
	 	hdfs.delete(output2, true);
  	  }
        }
	//This job is used for sorting the key values according to the page rank values
	if(result)
	{
	   Job job3  = Job .getInstance(getConf(), " pagerank ");
      	   job3.setJarByClass( this .getClass());
      	   FileInputFormat.addInputPaths(job3,  args[1]);
      	   FileOutputFormat.setOutputPath(job3,  new Path(args[2]));
	   job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
      	   job3.setMapperClass( Map3 .class);
      	   job3.setReducerClass( Reduce3 .class);
      	   job3.setOutputKeyClass( DoubleWritable .class);
      	   job3.setOutputValueClass( Text .class);
	   job3.setNumReduceTasks(1);
	   result = job3.waitForCompletion( true);
	}
	return result ? 0 : 1;
   }
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private static final Pattern TITLE_TAG = Pattern .compile("<title>(.+?)</title>"); //This pattern is used for retrieve the data between title tags
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
         String line  = lineText.toString();
         Text currentWord  = new Text();
	 String inputFileName = new String();
	 Matcher matcher = TITLE_TAG.matcher(line); // This matches the title tag in the given line
	 String title = "";
	 //Below is to fetch the title from the line
	 while(matcher.find())
	 {
		title = matcher.group(1).trim();
	 }
	 if(title.length()>0){
	 currentWord  = new Text("titleCount");
	 context.write(currentWord,one);
	 }
       }
   }
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
	 Configuration conf = context.getConfiguration();
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
	 context.write(word,new IntWritable(sum));
      }
   }
   
   public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
      private static final Pattern TITLE_TAG = Pattern .compile("<title>(.+?)</title>"); //This pattern is used for retrieve the data between title tags
      private static final Pattern TEXT_TAG = Pattern .compile("<text[^>]*>(.+?)</text>");//This pattern is used for retrieve the data between text tags
      private static final Pattern LINK_TAG = Pattern .compile("\\[\\[(.+?)\\]\\]");//This pattern is used for retrieve the data between [[ ]] tags

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
	 double pagerank;
	 int n=0;
         String line  = lineText.toString();
	 List<String> urlList = new ArrayList<String>(); // To store the URL list or output link list for each node/title
	 Matcher matcher = TITLE_TAG.matcher(line); // This matches the title tag in the given line
	 String title = "";
	 //Below is to fetch the title from the line
	 while(matcher.find())
	 {
		title = matcher.group(1).trim();
	 }
	 if(title.length()>0){
		 matcher = TEXT_TAG.matcher(line);
		 matcher.matches();
		 String text = "";
		 while(matcher.find())
		 {
			text = matcher.group(1).trim();
		 }
		 matcher = LINK_TAG.matcher(text);
		 String output = "";
		 //Now, each outlink is extracted using this matcher and inserted in the list as well as a string is created by appending the outlinks
		 while(matcher.find())
		 {
		     urlList.add(matcher.group(1).trim());
		     output+=matcher.group(1).trim()+"#$#$";
		 }
		 n = urlList.size();
	         if(n>0)
		 {
			 pagerank = 1/(double)n;
			 for (int i=0;i<n;i++){
				context.write(new Text(urlList.get(i)), new Text(Double.toString(pagerank))); // 1/n value is being sent to the reducer where n is length of the outlinks
			 }
		 }
		context.write(new Text(title),new Text(output)); // all the outlinks seperated by the given delimiter is being sent to the reducer here.
	 }
	 
      }
   }
   public static class Reduce1 extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text> linksorpageranks,  Context context)
         throws IOException,  InterruptedException {
	 Configuration conf = context.getConfiguration();
	 double pagerank = 0.0;
	 double totalcount = Double.parseDouble(conf.get("TotalCount"));
	 String links="";
	 for ( Text linkorpagerank  : linksorpageranks) {
		//The below try catch is to check for the pagerank values or the outlinks list which is a string
         	try{
			//The pagerank rank value sent by the Map operation is multiplied by 1/N which is the initial pagerank value
			pagerank = pagerank + (double)1/totalcount*Double.parseDouble(linkorpagerank.toString().trim());
		}
		catch(NumberFormatException ex){
			//All the output links are being assigned to the links variable here
			links = linkorpagerank.toString();
		}
         }
	 pagerank = 0.15 + 0.85*(pagerank); // Actual page rank value is being calculated here.
	 String currentword = word.toString()+"#$#$"+Double.toString(pagerank)+"#$#$#$"; // The title(node) and its page rank is being seperated by the delimiters provided
	 //The below condition checks if there are any Dangling nodes and will ignore them in the display
	 if(links.length()>0){
		context.write(new Text(currentword), new Text(links));
	 }
      }
   }
   public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
	 double pagerank = 0.0;
	 int n =0;
         String line  = lineText.toString();
         String[] titlelinks = line.split("#\\$#\\$#\\$"); //The key and the value pair are seperated from the given input line.
	 String[] titlepagerank = titlelinks[0].trim().split("#\\$#\\$");//The title and page rank are seperated by the given delimeter 
	 String[] urlList = titlelinks[1].trim().split("#\\$#\\$"); // all the outlinks are seperated by this given delimiter
	 n = urlList.length; // this is to calculate the total number of outlinks for a given node
	 pagerank = Double.parseDouble(titlepagerank[1].trim())/n; //The page rank of the given node is divided by total number of outlinks equally
	 for ( String link  : urlList) {
		if(link.length()>0){
			context.write(new Text(link), new Text(Double.toString(pagerank))); // The shared page rank value and each outlink is being sent to reducer to calculate the over all pagerank
		}
	 }
         context.write(new Text(titlepagerank[0]),new Text(titlelinks[1])); // The outlink list is being sent to the reducer
         }
    }
    //This reducer performs the same as reducer 1
    public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text> linksorpageranks,  Context context)
         throws IOException,  InterruptedException {
	 double pagerank = 0.0;
	 String links="";
	 for ( Text linkorpagerank  : linksorpageranks) {
         	try{
			pagerank = pagerank + Double.parseDouble(linkorpagerank.toString().trim());
		}
		catch(NumberFormatException ex){
			links = linkorpagerank.toString();
		}
         }
	 pagerank = 0.15 + 0.85*(pagerank);
	 String currentword = word.toString()+"#$#$"+Double.toString(pagerank)+"#$#$#$";
	 //The below condition checks if there are any Dangling nodes and will ignore them in the display
	 if(links.length()>0){
		context.write(new Text(currentword), new Text(links));
	 }
      }
   }
   //This map operation will take the title(node) and pagerank value and send it to the reducer
   public static class Map3 extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
      
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
	 double pagerank = 0.0;
	 int n =0;
         String line  = lineText.toString();
         String[] titlelinks = line.split("#\\$#\\$#\\$"); // This splits between the key and value of the previous map reduce function
	 String[] titlepagerank = titlelinks[0].split("#\\$#\\$"); // This splits between the node and page rank value
	 pagerank = Double.parseDouble(titlepagerank[1].trim());
         context.write(new DoubleWritable(pagerank),new Text(titlepagerank[0].trim()));
         }
    }
   //The below reducer will print the key and values into the output file by sorting them.
   public static class Reduce3 extends Reducer<DoubleWritable ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( DoubleWritable rank,  Iterable<Text > links,  Context context)
         throws IOException,  InterruptedException {
         for ( Text link  : links) {
            context.write(link, new Text(rank.toString()));
         }
      }
   }
   
}
