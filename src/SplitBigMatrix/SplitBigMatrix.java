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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import SplitBigMatrix.MapToSmallMatrix;

public class SplitBigMatrix {

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
		smallMatrix = smallMatrix.reduceByKey(new Function2<String, String, String>() {

			@Override
			public String call(String arg0, String arg1) throws Exception {
				return arg0 + "\n" + arg1;
			}
		
		});
		List<Tuple2<String, String>> output = smallMatrix.collect();
		System.out.println("ready to print");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream outFile = null;
		Path dir = new Path("/user/zzq/"+args[3]);
		fs.mkdirs(dir);
		for (Tuple2<String, String> t: output) {
			outFile = fs.create(dir.suffix("/" + t._1()));
			outFile.writeBytes(t._2().toString() + "\n");
			outFile.close();
		}
		System.exit(0);
	}

}
