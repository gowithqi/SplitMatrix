package SplitBigMatrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import SplitBigMatrix.MapToSmallMatrix;

public class SplitBigMatrix {
	
	private static class myP extends Partitioner {

		@Override
		public int getPartition(Object arg0) {
			// TODO Auto-generated method stub
			String s = arg0.toString();
			int i = Integer.parseInt(s.substring(12, 13));
			int j = Integer.parseInt(s.substring(14, 15));
			
			return i*10 + j;
		}

		@Override
		public int numPartitions() {
			// TODO Auto-generated method stub
			return 60;
		}
		
	}
	private static class writeToDfs extends Function<Tuple2<String, String>, Integer> {
		private String dir;
		public writeToDfs(String _dir) {
			dir = _dir;
		}
		@Override
		public Integer call(Tuple2<String, String> t) throws Exception {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream outFile = null;
			
			Path p = new Path(dir + "/" + t._1());
			outFile = fs.append(p);
			outFile.writeBytes(t._2() + "\n");
			outFile.close();
			return 1;
		}
		
	}
	public static void main(String[] args) throws IOException {
		System.out.println("Main start: " + args[0] + args[1] + args[2] + args[3]);
		// TODO Auto-generated method stub
		if (args.length<4) {
		      System.err.println("Need three arguments <master> <file> <number> <output dir>");
		      System.exit(1);
		}
		JavaSparkContext ctx = new JavaSparkContext(args[0], "SplitBigMatrix", 
				System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(SplitBigMatrix.class));
		
		JavaRDD<String> lines = ctx.textFile(args[1]);
		if (lines == null) {
			System.out.println("Does not exist such a file: " + args[1]);
			System.exit(0);
		}
		Integer n = Integer.getInteger(args[2]);
		System.out.println("Before map!");
		MapToSmallMatrix mapClass = null;
		try {
			mapClass = new MapToSmallMatrix();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (mapClass == null) {
			System.out.println("map class is null");
			System.exit(0);
		}
		//mapClass.SetN(n);
		JavaPairRDD<String, String> smallMatrix =	
				lines.flatMap(mapClass);
		
		System.out.println("map sucess!");
		
		//JavaPairRDD<String, List<String>> smallMatrix = _smallMatrix.groupByKey();
//		smallMatrix = smallMatrix.reduceByKey(new Function2<String, String, String>() {
//
//			@Override
//			public String call(String arg0, String arg1) throws Exception {
//				return arg0 + "\n" + arg1;
//			}
//		
//		});
		smallMatrix = smallMatrix.partitionBy(new myP());
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream outFile = null;
		Path dir = new Path("/user/zzq/"+args[3]);
		if (!fs.exists(dir)) fs.mkdirs(dir);
		List<String> fileNames = smallMatrix.keys().distinct().collect();
		for (String filename: fileNames) {
			System.out.println(filename);
			outFile = fs.create(dir.suffix("/" + filename), true);
			List<String> ls = smallMatrix.lookup(filename);
			for (int i = 0; i < ls.size(); i++) {
				String w = ls.get(i);
				if (i != ls.size()-1) w = w + "\n";
				outFile.writeBytes(w);
			}
			outFile.close();
		}
		
		//System.out.println(smallMatrix.map(new writeToDfs("/user/zzq/"+args[3])).count());
	
		System.exit(0);
	}

}
