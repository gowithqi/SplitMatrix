package SplitBigMatrix;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import scala.Tuple3;

import java.lang.Math;
import java.util.ArrayList;
import java.util.HashMap;

public class MapToSmallMatrix 
extends PairFlatMapFunction<String, String, String>{
	// n = k*k + m;
	private int n;
	private int k;
	private int m;
	
	public void SetN(int nn) {
		System.out.println("SetN");
		n = nn; 
		k = 3;
		m = n - k*k;
	}
	
	private int computeMatrixIOrJ(int p) {
		int sum = 5716808;
		int divide = 3;
		k = sum / divide;
		
		int ret = p / k + 1;
		if (ret > divide) ret = divide;
		return ret;
	}
	
	@Override
	public Iterable<Tuple2<String, String>> call(
			String line) {
		System.out.println("I'm a map!");
		ArrayList<Tuple2<String, String>> ret = 
				new ArrayList<Tuple2<String, String>>();
		
		String[] strings = line.split(" ");
		HashMap<Integer, String> map = new HashMap<Integer, String>();
		Integer i = Integer.valueOf(strings[0].substring(0, strings[0].length()-1));
		Integer matrixI = computeMatrixIOrJ(i);
		for (int k = 1; k < strings.length; k++) {
			Integer j = Integer.valueOf(strings[k]);
			Integer matrixJ = computeMatrixIOrJ(j);
			if (map.containsKey(matrixJ)) {
				map.put(matrixJ, map.get(matrixJ).concat(j.toString() + " "));
			} else {
				map.put(matrixJ, "");
				map.put(matrixJ, map.get(matrixJ).concat(j.toString() + " "));
			}
		}
		
		for (Integer matrixJ : map.keySet()) {
			String key = "SmallMatrix_" + matrixI.toString() + "_" + matrixJ.toString();
			String value = i.toString() + " " + String.valueOf(strings.length - 1) + " " + map.get(matrixJ);
			System.out.println(key + " __ " + value);
			ret.add(new Tuple2<String, String>(key, value));
		}
		
		return ret;
	}
	
}
