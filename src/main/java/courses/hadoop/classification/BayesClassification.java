package courses.hadoop.classification;

import courses.hadoop.hdfs.FsOperation;
import courses.hadoop.model.BayesModel;
import courses.hadoop.model.Vocabulary;

public class BayesClassification {
	
	public static void main(String[] args) throws Exception {
		// 初始化
		String master = "hdfs://172.16.91.1:9000";
		String data = System.getProperty("user.dir")+"/src/Data";
		String path = master + "/Bayes";
		String[] Class = {"AUSTR", "INDIA"};
		int[] trainNum = {244,261};
		
		//上传训练数据
		FsOperation.UpLoad(master, data+"/input", "/Bayes");
		
		//训练集建模
		if(!BayesModel.CreateModel(master, path, Class)){
			System.out.println("建模失败！");
			System.exit(0);
		}
		
		//测试集预处理
		BayesModel.DataProcessing(master, path+"/input/test", path, Class);
		
		//类别词汇数和词汇表统计
		Vocabulary.Cal(master, path, Class);
		
		//下载模型相关数据
		FsOperation.DownLoad(master, path+"/output", data);
		
		//对模型进行评估
		BayesModel.ForeCast(data, Class, trainNum);
		
		System.exit(1);
	}
}
