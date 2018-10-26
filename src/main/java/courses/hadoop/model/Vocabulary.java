package courses.hadoop.model;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import courses.hadoop.hdfs.FsOperation;
import courses.hadoop.model.BayesModel.IntSumReducer;
import courses.hadoop.model.BayesModel.TokenizerMapper;

public class Vocabulary {
	
	public static class WordMapper 
			extends Mapper<Object, Text, Text, IntWritable> {
		
		private static final IntWritable One = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text("11"), One);
			}
		}
	
	public static class WordReduce
			extends Reducer<Text, IntWritable, NullWritable, IntWritable> {
		
		private static IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> Value, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val:Value) {
				sum += val.get();
			}
			result.set(sum);
			context.write(NullWritable.get(), result);
		}
	}
	
	public static Boolean count(FileSystem fSystem, String name, String input, String output) throws Exception {
		Configuration configuration = new Configuration();
		if(fSystem.exists(new Path(output+"/"+name))) {
			System.out.println("类别"+name+"词数已统计，重新处理请删除目录：" + output+"/"+name);
			return true;
		}
		Job job = Job.getInstance(configuration, name);
		job.setJarByClass(Vocabulary.class);
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input+"/"+name));
		FileOutputFormat.setOutputPath(job, new Path(output+"/"+name));
		if(job.waitForCompletion(true)) {
			System.out.println("词汇表："+ name +"统计完成!");
			return true;
		}else {
			System.out.println("词汇表："+ name +"统计失败!");
			return false;
		}
	}
	
	public static Boolean TotalVoca(String master,String name, String input, String output) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(URI.create(master), configuration);
		if(fSystem.exists(new Path(output+"/"+name))) {
			System.out.println("总词汇表(第一步)已统计，重新处理请清空目录：" + output+"/"+name);
			return true;
		}
		Job job = Job.getInstance(configuration, "Total Vocabulary process 1");
		job.setJarByClass(BayesModel.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output+"/"+name));
		if(!job.waitForCompletion(true)) {
			return false;
		}
		FsOperation.DeletFile(master, output+"/"+name+"/"+"_SUCCESS");
		return true;
	}
	
	public static void Cal(String master, String root, String[] Class) throws Exception {
		FsOperation.MakeDir(master, root + "/output/vocabulary");
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(URI.create(master), configuration);
		count(fSystem, Class[0], root+"/input/train", root+"/output/vocabulary");
		count(fSystem, Class[1], root+"/input/train", root+"/output/vocabulary");
		String paths = root+"/input/train/"+Class[0]+","+root+"/input/train/"+Class[1];
		if(TotalVoca(master,"TOTAL", paths, root+"/output") & count(fSystem, "TOTAL", root+"/output", root+"/output/vocabulary")) {
			System.out.println("总词汇表TOTAL统计完成!");
		}
	}
}
