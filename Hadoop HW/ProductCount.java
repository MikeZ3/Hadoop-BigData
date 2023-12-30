import java.io.IOException;
import java.util.*;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductCount extends Configured implements Tool {
	public static long threshold;

	public static class Mapper1 extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text combination = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Set<String> wordsSet = new HashSet<String>();
			StringTokenizer itr = new StringTokenizer(value.toString());
			int count = itr.countTokens();
			while(itr.hasMoreTokens()) {
				wordsSet.add(itr.nextToken());
			}
			ImmutableSet<String> immutableCopyWordsSet = ImmutableSet.copyOf(wordsSet);
			Set<Set<String>> combinations;
			for (int i = 0; i < count + 1; i++) {
				combinations = Sets.combinations(immutableCopyWordsSet, i);
				for (Set<String> itr2 : combinations) {
					if (!itr2.isEmpty()) {
						combination.set((String.join(" ", itr2 + "")));
						context.write(combination, one);
					}
				}
			}
		}
	}

	public static class Combiner1 extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class Reducer1 extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		private LongWritable thr = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			Configuration conf = context.getConfiguration();
			thr.set(conf.getLong("threshold", threshold));
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			if (result.compareTo(thr) > 0 || result.compareTo(thr) == 0) {
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ProductCount(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		threshold = Long.parseLong(args[2]);
		conf.setLong("threshold", threshold);

		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC,org.apache.hadoop.io.compress.SnappyCodec.class,CompressionCodec.class);
		
		Job job = Job.getInstance(getConf(), "ProductsCount");
		job.setJarByClass(ProductCount.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Combiner1.class);
		job.setReducerClass(Reducer1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		return returnValue;

	}
}
