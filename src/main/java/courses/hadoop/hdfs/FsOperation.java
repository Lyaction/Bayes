package courses.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FsOperation {

	public void UpLoad(String master, String sour, String des) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fSystem = FileSystem.get(new URI(master), configuration);
		InputStream in = new FileInputStream(sour);
		OutputStream out = fSystem.create(new Path(des), new Progressable() {
			
			public void progress() {
				// TODO Auto-generated method stub
				System.out.println("创建成功！");
			}
		});
		IOUtils.copyBytes(in, out, configuration);
	}
	
	public void DownLoad(String master, String sour, String des) {
		
	}
}
