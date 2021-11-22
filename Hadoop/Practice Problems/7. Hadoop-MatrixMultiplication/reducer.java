package matrixmul;
import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterator<Text> value1, OutputCollector<Text, Text> values, Reporter r) throws IOException {
			String[] value;
			HashMap<Integer, Float> hashA = new HashMap<Integer,Float>();
			HashMap<Integer, Float> hashB = new HashMap<Integer,Float>();

			while(value1.hasNext()){
				value = value1.next().toString().split(",");
				if (value[0].equals("A")) {
					hashA.put(Integer.parseInt(value[1]),
					Float.parseFloat(value[2]));
				} else {
					hashB.put(Integer.parseInt(value[1]),
					Float.parseFloat(value[2]));
				}
			}
			int n = 5;
			float result = 0.0f;
			float a_ij;
			float b_jk;
			for (int j = 0; j < n; j++) {
				a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
				b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
				result += a_ij * b_jk;
			}
			if (result != 0.0f) {
				values.collect(null, new Text(key.toString() + "," + Float.toString(result)));
			}
	}
}
