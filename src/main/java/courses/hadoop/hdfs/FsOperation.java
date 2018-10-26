package courses.hadoop.hdfs;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FsOperation {

	public static void UpLoad(String master, String sour, String des) throws Exception {
		MakeDir(master, des);
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(new URI(master), configuration);
		FileStatus[] liStatus = fSystem.listStatus(new Path(des));
		if (liStatus.length==0) {
			System.out.println("正在上传训练数据...............");
			fSystem.copyFromLocalFile(new Path(sour), new Path(des));
			fSystem.close();
			System.out.println("上传成功！");	
		}else {
			System.out.println(master+des+":上传完毕!");
		}
	}
	
	public static void DownLoad(String master, String sour, String des) throws Exception {
		File file = new File(des+"/output");
		if(file.exists()) {
			System.out.println("文件已下载，或者删除目录：" + des + "/output !");
		}else {
			Configuration configuration = new Configuration();
			FileSystem fSystem = FileSystem.get(new URI(master), configuration);
			System.out.println("正在下载模型及测试数据...............");
			fSystem.copyToLocalFile(new Path(sour), new Path(des));
			System.out.println("下载完成！模型及测试集在目录："+ des + "/output 下！");
		}
	}
	
	public static void MakeDir(String master, String des) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fSystem = FileSystem.get(URI.create(master), conf);
		fSystem.mkdirs(new Path(des));
		System.out.println("创建文件夹：" + des);
	}
	
	public static void DeletFile(String master, String des) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fSystem = FileSystem.get(URI.create(master), conf);
		fSystem.delete(new Path(des), true);
		System.out.println("删除文件：" + des);
	}
}
