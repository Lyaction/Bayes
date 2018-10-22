package courses.hadoop.model;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BayesModel {

	public static class TokenizerMapper 
			extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable  One = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context
					)throws IOException, InterruptedException {
			StringTokenizer iTokenizer = new StringTokenizer(value.toString());
			while(iTokenizer.hasMoreTokens()) {
				word.set(iTokenizer.nextToken());
				context.write(word, One);
			}
		}
	}
	
	public static class IntSumReducer
			extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> value, Context context
						) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val:value) {
				sum+=val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static Boolean CreateModel() throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "Model Data");
		job.setJarByClass(BayesModel.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		String path = "src/Data";
		String[] Class = {"AUSTR", "INDIA"};
		for(String val:Class) {
			FileInputFormat.addInputPath(job, new Path(path+"/input/"+val));
			FileOutputFormat.setOutputPath(job, new Path(path+"/output/"+val));
			if(!job.waitForCompletion(true)) {
				System.out.println(val+":fail!");
				return false;
			}
			System.out.println(val+":success!");
		}
		return true;
	}
}
