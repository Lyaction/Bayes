package courses.hadoop.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import courses.hadoop.hdfs.FsOperation;

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
	
	public static Boolean WordCount(Configuration configuration, String input, String output, String Class) throws Exception {
		Job job = Job.getInstance(configuration, "Bayes Model");
		job.setJarByClass(BayesModel.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input+"/"+Class));
		FileOutputFormat.setOutputPath(job, new Path(output+"/"+Class));
		if(!job.waitForCompletion(true)) {
			System.out.println(Class+":训练失败!");
			return false;
		}
		System.out.println(Class+":训练成功!");
		return true;
	}
	
	public static Boolean CreateModel(String master, String path, String[] Class) throws Exception {
		FsOperation.MakeDir(master, path+"/output");
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(URI.create(master), configuration);
		FileStatus[] listFiles = fSystem.listStatus(new Path(path + "/output"));
		if(listFiles.length!=0) {
			System.out.println("Bayes Model已经存在，重新建模请清空目录：" + path + "/output");
			return true;
		}
		if (WordCount(configuration, path+"/input/train", path+"/output", Class[0]) 
				& WordCount(configuration, path+"/input/train", path+"/output", Class[1])) {
			return true;
		}
		return false;
	}
	
	public static Boolean DataProcessing(String master, String input, String output, String[] Class) throws Exception {
		FsOperation.MakeDir(master, output+"/output/test");
		output = output + "/output/test";
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(URI.create(master), configuration);
		FileStatus[] listFiles = fSystem.listStatus(new Path(output));
		fSystem.close();
		if(listFiles.length!=0) {
			System.out.println("测试数据已处理，重新处理请清空目录：" + output);
			return true;
		}
		for(String val:Class) {
			fSystem = FileSystem.get(URI.create(master), configuration);
			FileStatus[] liStatus = fSystem.listStatus(new Path(input+"/"+val));
			fSystem.close();
			System.out.println("正在对类别"+val+"进行处理,共计"+liStatus.length+"个文件");
			for(FileStatus file:liStatus) {
				String name = file.getPath().getName();
				if(!WordCount(configuration, input+"/"+val, output+"/"+val, name)) {
					System.out.println("文件："+file.getPath()+" 处理失败！");
					return false;
				}
			}
			fSystem.close();
		}
		System.out.println("测试集处理完毕！");
		return true;
	}
	
	public static void ForeCast(String path, String[] Class, int[] sampleNum) {
		int vocabulary = 0;
		float TP = 0;int TN = 0;
		int FN = 0;int FP = 0;
		Map<String, Object> wordNum = new HashMap<String, Object>();
		float trainNumFirst = new File(path+"/input/train/"+Class[0]).list().length;
		float trainNumSec = new File(path+"/input/train/"+Class[1]).list().length;
		path = path + "/output";
		BufferedReader reader = null;
		Map<String, Map<String, Object>> listMap = new HashMap<String, Map<String, Object>>();
		
		//读词汇表
		try {
			reader = new BufferedReader(new FileReader(path+"/vocabulary/TOTAL"+"/part-r-00000"));
			vocabulary = Integer.parseInt(reader.readLine());reader.close();
			reader = new BufferedReader(new FileReader(path+"/vocabulary/"+Class[0]+"/part-r-00000"));
			wordNum.put(Class[0],vocabulary + Integer.parseInt(reader.readLine()));reader.close();
			reader = new BufferedReader(new FileReader(path+"/vocabulary/"+Class[1]+"/part-r-00000"));
			wordNum.put(Class[1], vocabulary + Integer.parseInt(reader.readLine()));reader.close();
		}catch (IOException e) {
			e.printStackTrace();
		}
		
		//读取模型
		for(String val:Class) {
			Map<String, Object> map = new HashMap<String, Object>();
			String temp = null;
			try {
				reader = new BufferedReader(new FileReader(path+"/"+val+"/part-r-00000"));
				while((temp = reader.readLine()) != null) {
					String[] line = temp.split("\t");
					map.put(line[0], line[1]);
				}
				listMap.put(val, map);
				reader.close();
			}catch (IOException e) {
				e.printStackTrace(); 
			}
		}
		
		//对模型进行评估
		System.out.println("假定类别"+Class[1]+"标识为0,"+Class[0]+"标识为1");
		float precision = 0;float recall = 0;float f1 = 0;
		path = path + "/test";
		int power = 0; //次方
		for(String val:Class) {
			String Dir = path + "/" +val;
			String[] fileList = new File(Dir).list();
			for(String file:fileList) {
				String temp = null;
				float pro = trainNumFirst / trainNumSec;
				try {
					reader = new BufferedReader(new FileReader(Dir+"/"+file+"/part-r-00000"));
					while((temp = reader.readLine()) != null) {
						float wordClassFirst = 0; //单词在某一类中出现的次数
						float wordClassSec = 0; 
						String[] line = temp.split("\t");
						power = Integer.parseInt(line[1]);
						try {
							wordClassFirst = Integer.parseInt((String)listMap.get(Class[0]).get(line[0]));
						}catch (NumberFormatException e) {
							wordClassFirst = 0;
						}
						try {
							wordClassSec = Integer.parseInt((String)listMap.get(Class[1]).get(line[0]));
						}catch (NumberFormatException e) {
							wordClassFirst = 0;
						}
						pro *= Math.pow(((wordClassFirst+1) / Integer.parseInt(String.valueOf((wordNum.get(Class[0]))))), power) 
								/ Math.pow(((wordClassSec+1) / Integer.parseInt(String.valueOf((wordNum.get(Class[1]))))), power);
					}
					reader.close();
					if(pro >= 1) {
						if(val.equals(Class[0])) {
							TP++;
						}else {
							FP++;
						}
					}else {
						if(val.equals(Class[1])) {
							TN++;
						}else {
							FN++;
						}
					}
				}catch (IOException e) {
					e.printStackTrace();
				}	
			}
		}
		precision = TP / (TP+FP);
		recall = TP / (TP+FN);
		f1 = 2*precision*recall / (precision+recall);
		System.out.println("精准度（Precision）:" + precision);
		System.out.println("召回率（Recall）:" + recall);
		System.out.println("准确率（Accuracy）:" + f1);
	}
}