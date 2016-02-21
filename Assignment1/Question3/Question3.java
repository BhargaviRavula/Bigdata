import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question3 {

	public static class TopTenMap
	extends Mapper<LongWritable, Text, Text, FloatWritable>{

		private Text business_Id = new Text();  // type of output key
		private FloatWritable star = new FloatWritable(); // type of output value


		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] data = value.toString().split("\\^");
			business_Id.set(data[2]);   // set business_id as each input keyword
			star.set(Float.parseFloat(data[3]));
			context.write(business_Id, star);
		}
	}

	public static class TopTenReduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		HashMap<String, Float> reducersideMap = new HashMap<String, Float>();

		public void reduce(Text key, Iterable<FloatWritable> values, Context context
				) throws IOException, InterruptedException {
			float starSum = 0; // initialize the sum for each business's ratings
			int totalRatings = 0;
			for (FloatWritable val : values) {
				starSum += val.get();
				totalRatings++;
			}

			float avgRating = starSum/totalRatings; // average rating of each business
			reducersideMap.put(key.toString(), new Float(avgRating));
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Float> sortedMap = new TreeMap<String, Float>(
					new Comparator<String>() {
						@Override
						public int compare(String key1, String key2) {
							int compare = reducersideMap.get(key2).compareTo(reducersideMap.get(key1));
							if (compare == 0)
								return 1;
							return compare;
						}
					});
			sortedMap.putAll(reducersideMap);
			int counter = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
				counter++;
				context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
				if (counter == 10)
					break;
			}
		}
	}

	public static class TopTenJoinMap extends Mapper<LongWritable, Text, Text, Text>{

		private Text business_Id = new Text();  // type of output key
		private Text avgRating = new Text(); // type of output value

		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\t");
			business_Id.set(mydata[0]);   // set business_id as each input keyword
			avgRating.set("tt:\t" + mydata[1]);
			context.write(business_Id, avgRating);
		}
	}

	public static class TopTenJoinReduce extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context
				) throws IOException, InterruptedException {
			boolean flag = false;
			String str1 = "";
			String str2 = "";
			String details = "";

			for (Text val : values) {
				if (val.toString().contains("ac:"))
					str1 = val.toString();
				else if (val.toString().contains("tt:")) {
					str2 = val.toString();
					flag = true;
				}
			}			
			if (flag) {
				String[] splitStr1 = str1.split("\t");
				details = splitStr1[1] + "\t" + splitStr1[2] + "\t" + str2.split("\t")[1];
				context.write(key, new Text(details));
			}
		}
	}

	public static class BusinessMap extends Mapper<LongWritable, Text, Text, Text>{		
		private static List<String> business_Ids = new ArrayList<String>();

		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] mydata = value.toString().split("\\^");			
			String businessDetails = "";

			if (!business_Ids.contains(mydata[0])) {				
				business_Ids.add(mydata[0]);
				businessDetails = "ac:\t" + mydata[1] + "\t" + mydata[2];
				context.write(new Text(mydata[0]), new Text(businessDetails));
			}			
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Question3 <in> <out>");
			System.exit(3);
		}

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "averageRating");
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf,"reduceSideJoin");
		job1.setJarByClass(Question3.class); 
		job1.setMapperClass(TopTenMap.class);
		job1.setReducerClass(TopTenReduce.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));    
		FileOutputFormat.setOutputPath(job1, new Path("/bxr140530/Asgn/temp"));

		if(job1.waitForCompletion(true))
		{
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setJarByClass(Question3.class);
			job2.setMapperClass(TopTenJoinMap.class);
			job2.setReducerClass(TopTenJoinReduce.class);

			MultipleInputs.addInputPath(job2,new Path("/bxr140530/Asgn/temp"),TextInputFormat.class,TopTenJoinMap.class);
			MultipleInputs.addInputPath(job2,new Path(otherArgs[1]),TextInputFormat.class,BusinessMap.class);
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

			job2.waitForCompletion(true);


		}
	}
}
