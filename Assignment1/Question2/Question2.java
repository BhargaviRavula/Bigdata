
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question2{

	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String business_id = value.toString().split("\\^")[2];
			String stars = value.toString().split("\\^")[3];

			context.write(new Text(business_id), new FloatWritable(Float.parseFloat(stars)));
		}	

	}


	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{

		public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {

			float total = 0;
			int count = 0;
			for (FloatWritable val : values) {
				//String[] splitval = star.toString().split("\\^");
				//if(splitval[0].equalsIgnoreCase("Average")){
				total += val.get();
				count++;
			}

			float average = total/count;
			context.write(new Text(key.toString()), new FloatWritable(1/average));
		}
	}

	public static class JoinMap extends Mapper<LongWritable, Text, FloatWritable, Text>{

		private Text business_id = new Text();
		private FloatWritable avg= new FloatWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] data = value.toString().split("\t");
			business_id.set(new Text(data[0]));
			avg.set(Float.parseFloat(data[1]));
			context.write(avg, business_id);
		}
	}

	public static class JoinReduce extends Reducer<FloatWritable, Text, Text, NullWritable>{

		private static int count=0;

		public void reduce(FloatWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException{
			for(Text val : values){
				count++;
				if(count <= 10){
					context.write(val, null);
				}
				else
					break;
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: review.csv <in> <out>");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "Question2");
		job1.setJarByClass(Question2.class);
		job1.setMapperClass(Map.class);
		job1.setReducerClass(Reduce.class);


		// set output key type 
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(FloatWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job1, new Path("/bxr140530/Asgn/Q2IntermediateOutput"));

		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "join");
		if(job1.waitForCompletion(true)){
			job2.setOutputKeyClass(FloatWritable.class);
			job2.setOutputValueClass(Text.class);
			job2.setJarByClass(Question2.class);
			job2.setMapperClass(JoinMap.class);
			job2.setReducerClass(JoinReduce.class);

			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job2, new Path("/bxr140530/Asgn/Q2IntermediateOutput"));
			// set the HDFS path for the output 
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

			job2.waitForCompletion(true);
		}

	}	

}
