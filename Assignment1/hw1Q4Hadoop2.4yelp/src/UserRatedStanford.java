
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UserRatedStanford{
	

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		private Text rating = new Text();  // type of output key 
		private Text userid = new Text();
		
		HashMap<String,String> myMap;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//from ratings
			
			String[] mydata = value.toString().split("\\^");
		
				
				if( myMap.get(mydata[2].trim()).split("\\^")[1].contains("Stanford")== true){ //
						userid.set(mydata[1]);
						rating.set(mydata[3]);	
						context.write(userid ,rating);
				
					
				
			}
			
			
			
		}
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stu
			super.setup(context);
			//read data to memory on the mapper.
			myMap = new HashMap<String,String>();
			Configuration conf = context.getConfiguration();
			String mybusinessdataPath = conf.get("businessdata");
			//e.g /user/hue/input/
			Path part=new Path("hdfs://cshadoop1"+mybusinessdataPath);//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split("\\^");
		        	if(arr.length == 3){
		            myMap.put(arr[0].trim(), line); //businessid and the remain datacolumns
		        	}
		            line=br.readLine();
		        }
		       
		    }
			
			
			
	       
		}
		
		
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
		
		/*	int count=0;
			for(Text t : values){
				count++;
			}
			Text myCount = new Text();
			myCount.set(""+count);
			context.write(key,myCount);*/
			Text myCount = new Text();
			for(Text t : values){
				myCount.set(""+t);
				context.write(key,myCount);
			}
		}
	}

// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: UserRatedStanford <inbusiness> <review> <out>");
			System.exit(2);
		}
		
	//	DistributedCache.addCacheFile(new URI("hdfs://cshadoop1"+ otherArgs[1]), conf);       
		
		conf.set("businessdata", otherArgs[0]);
		
		Job job = new Job(conf, "UserRatedStanford");
		job.setJarByClass(UserRatedStanford.class);
		
	//	 final String NAME_NODE = "hdfs://localhost:9000";
    //    job.addCacheFile(new URI(NAME_NODE
	//	    + "/user/hduser/"+otherArgs[1]+"/users.dat"));
	/*	final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
	        job.addCacheFile(new URI(NAME_NODE
			    + otherArgs[1]));
	*/	
	  //    final String NAME_NODE = "hdfs://cshadoop1";
	  //      job.addCacheFile(new URI(NAME_NODE
	//		    + otherArgs[1]));
		
		
		job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(0);
//		uncomment the following line to add the Combiner
//		job.setCombinerClass(Reduce.class);
		
		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}