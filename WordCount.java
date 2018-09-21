package cs6360.assignments.assignment2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {

	  public static class TokenizerMapper
	       extends Mapper<Object, Text, Text, IntWritable>{
		  
//		  removing stop words
		  HashSet <String[]>stopWords = new HashSet<String[]>();
		  public void getStopWords() throws IOException {
			  
			  Configuration conf = new Configuration();
			  conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		      conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
		      FileSystem fs = FileSystem.get(conf); 
		    	
		      Path p = new Path("hdfs://cshadoop1/user/dxp172730/stopWords.txt");
		      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(p)));
		      String line;
		      line=br.readLine();
		      line.replaceAll("[\']", "");
		      while (line != null){
		    	  stopWords.add(line.split(","));
		          line=br.readLine();
		      }
		  }

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text(); 
	    
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	 	
//	    getting rid of all characters other than alphabets
	      String str = value.toString().replaceAll("[^a-zA-Z \\n]", "");
//	      creating tokens
		  StringTokenizer itr = new StringTokenizer(str.toLowerCase());
		  while (itr.hasMoreTokens()) {
			  word.set(itr.nextToken());
//			  checking if the length of the token is greater than 5 and if it is one of the given stop words
			  if((word.getLength() > 5) && !(stopWords.contains(word))) {
				  context.write(word, one);
			  }
		  }
	    }
	  }

	  public static class IntSumReducer
	       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	    FileSystem fs = FileSystem.get(conf); 
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
//	    using all the files downloaded in assignment1 as input to the mapper class 
	    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
	            new Path(args[0]), true);
	    while(fileStatusListIterator.hasNext()){
	        LocatedFileStatus fileStatus = fileStatusListIterator.next();
	        MultipleInputs.addInputPath(job, fileStatus.getPath(), TextInputFormat.class);
	    }
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	
}
