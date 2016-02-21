import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

@SuppressWarnings("deprecation")
public class Question4 {

	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

		private static List<String> Business = new ArrayList<String>();

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();
			String myfilepath = conf.get("cachefile");
			Path part = new Path("hdfs://cshadoop1"+myfilepath);

			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);

			for(FileStatus status: fss){
				Path pt = status.getPath();

				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line = null;
				while((line = br.readLine())!=null){
					String[] businessData = line.split("\\^");
					if(businessData[1].contains("Stanford") && !Business.contains(businessData[0]))
						Business.add(businessData[0]);
				}	
			}
		}


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] reviewData = value.toString().split("\\^");
			if(Business.contains(reviewData[2]))
				context.write(new Text(reviewData[1]), new FloatWritable(Float.parseFloat(reviewData[3])));
		}
	}


	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			for(FloatWritable val : values) {
				context.write(key, val);
			}
		}
	}

	public static void main(String args[]) throws IOException,InterruptedException, ClassNotFoundException, URISyntaxException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("cachefile", otherArgs[0]);
		if (otherArgs.length != 3) {
			System.err.println("Usage: Question4 <cacheFile> <in> <out>");
			System.exit(3);
		}
		Job job = new Job(conf, "Question4");
		DistributedCache.addCacheFile(new URI(args[0]), conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class); 
		job.setJarByClass(Question4.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		job.waitForCompletion(true);
	}

}
