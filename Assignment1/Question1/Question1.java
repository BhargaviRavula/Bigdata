import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question1 {
	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{

		private Text business_Id = new Text();  
		private Text fullAddress = new Text(); 
				
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] mydata = value.toString().split("\\^");
			business_Id.set(mydata[0]);   
			fullAddress.set(mydata[1]);
			context.write(business_Id, fullAddress);    
		}
	}

	public static class Reduce
	extends Reducer<Text,Text,Text,NullWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context
				) throws IOException, InterruptedException {

			int locationIndex = -1;
			for (Text val : values) {
				locationIndex = val.toString().indexOf("Palo Alto"); 
			}
			
			if (locationIndex != -1)
				context.write(key, null); 
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: Question1 <in> <out>");
			System.exit(2);
		}

		// create a job with name "Question1" 
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Question1"); 
		job.setJarByClass(Question1.class); 
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type 
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}