package courses.hadoop.model;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


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
		
		
	}
}
