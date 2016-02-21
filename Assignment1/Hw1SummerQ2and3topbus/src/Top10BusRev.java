
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10BusRev{
	

	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
		private FloatWritable rating;
		private Text businessid = new Text();  // type of output key 
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\\^");
			//use distributed cache to load users and filter female ratings
			
			
			if (mydata.length == 4){
				
					businessid.set(mydata[2]);
					rating= new FloatWritable(Float.parseFloat(mydata[3]));
					context.write(businessid,rating);
				
			}	
			
			
		}
		
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
		
		
		}
		
		
	}

	public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key, Iterable<FloatWritable> values,Context context ) throws IOException, InterruptedException {
			float sum = 0; // initialize the sum for each keyword
			int count=0;
			for (FloatWritable val : values) {
				sum += val.get();
				count++;
			}
			result.set(sum/count);
			context.write(key, result); // create a pair <keyword, number of occurences>
		}
	}

	
	public static class MapTopRating extends Mapper<LongWritable, Text, Text, Text>{
		private Text rating;
		private Text businessid = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\t");
		///	System.out.println(value.toString());
			String intrating = mydata[1];
			rating = new Text("rat~" + intrating);
			businessid.set(mydata[0].trim());
			context.write(businessid, rating);
			
			
		}
		
		
	}
	
	
	public static class MapTopBusiness extends Mapper<LongWritable, Text, Text, Text>{
		private Text myAddress = new Text();
		private Text businessid = new Text();  // type of output key 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\\^");
			
			if (mydata.length == 3){
				
					System.out.println(value.toString());
					String address = mydata[0] + " " + mydata[1] + " "+mydata[2];
					myAddress.set("bus~" + address);
					businessid.set(mydata[0].trim());
					context.write(businessid, myAddress);
				
			}
			
		}
		
		
	}
	
	public static class ReduceTop extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		private Text myKey = new Text();
		
		ArrayList<MyBusinessData> myarray = new ArrayList<MyBusinessData>();
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {
		    String businessTitle="";
		    float myrating;
		    myrating = (float)0.0;
		    
		    // it is like mapreduce start garbage collecion as soon as you iterate through the values.
		    //no double iteration only nce
	    	for (Text val : values) {
				 
				//1 to 1 mapping if not mapping whould have to be copied to another array or collection
				String splitVals[] = val.toString().split("~");
				if (splitVals[0].trim().equals("bus")) {
					businessTitle = splitVals[1];
					
					
				}
				
				if (splitVals[0].trim().equals("rat")) {
					
					myrating = Float.parseFloat(splitVals[1]);
					
						
				}
			}
	    	//if(businessTitle.length() > 3){
		    	MyBusinessData temp = new MyBusinessData();
				temp.rating = myrating;
				temp.businessId = key.toString() + businessTitle;
				temp.businessAddress = "";
				myarray.add(temp);
	    	//}
			
		}
		@Override
		protected void cleanup(
				Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			Collections.sort(myarray,new MyMovieComparator());
			int count =0;
			for(MyBusinessData data : myarray){
				
				result.set(""+data.rating);
				myKey.set(data.businessId);
				context.write(myKey, result); // create a pair <keyword, number of occurences>
				count++;
				if(count >=10)break;}
			
		}
		class MyBusinessData{
			
			String businessId;
			Float rating;
			String businessAddress;
		}
		
		class MyMovieComparator implements Comparator<MyBusinessData> {
		    public int compare(MyBusinessData m1, MyBusinessData m2) {
		    	
		    	if(m1== null || m2== null){
		    		return 0;
		    	}
		    	if(m2.rating > m1.rating)return 1;
		    	if(m2.rating < m1.rating)return -1;		    	
		    	if(m2.rating == m1.rating)return 0;
		    	return 0;
		    }

			
		}
	}


// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: Top10BusRev <review> <fbusiness> <ooutput> ");
			System.exit(2);
		}
		// create a job with name "toptenratemov"
		Job job = new Job(conf, "Top10BusRev");
		job.setJarByClass(Top10BusRev.class);
		
		
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
//		uncomment the following line to add the Combiner
//		job.setCombinerClass(Reduce.class);
		
		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(FloatWritable.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path("/datatemp"));
		
		//Wait till job completion
		if(job.waitForCompletion(true) == true){
			
			// create a job with name "toptenratemov"
			Job job2 = new Job(conf, "Top10BusRev");
			
			job2.setJarByClass(Top10BusRev.class);
			job2.setReducerClass(ReduceTop.class);
			MultipleInputs.addInputPath(job2,  new Path("/datatemp"), TextInputFormat.class,
					MapTopRating.class);
			MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class,
					MapTopBusiness.class);
			
			
			
			
//			uncomment the following line to add the Combiner
//			job.setCombinerClass(Reduce.class);
			
			// set output key type 
			job2.setOutputKeyClass(Text.class);
			// set output value type
			job2.setOutputValueClass(Text.class);
			
			//set the HDFS path of the input data
			// set the HDFS path for the output 
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			job2.waitForCompletion(true);
			
		}
	}
}