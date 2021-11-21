package matrixmul;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> values, Reporter r) throws IOException {
		int m = 2;
		int p = 3;
		String line = value.toString();
		String[] indicesAndValue = line.split(",");
		Text outputKey = new Text();
		Text outputValue = new Text();
		if (indicesAndValue[0].equals("A")) {
			for (int k = 0; k < p; k++) {
				outputKey.set(indicesAndValue[1] + "," + k);
				outputValue.set("A," + indicesAndValue[2] + "," + indicesAndValue[3]);
				values.collect(outputKey, outputValue);
			}
		} else {
			for (int i = 0; i < m; i++) {
				outputKey.set(i + "," + indicesAndValue[2]);
				outputValue.set("B," + indicesAndValue[1] + "," + indicesAndValue[3]);
				values.collect(outputKey, outputValue);
			}
		}
	}
}
