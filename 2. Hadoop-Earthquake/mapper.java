package earthquake;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable>{
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> values, Reporter r) throws IOException {

		String[] line = value.toString().split(",", 12);
		if (line.length != 12) {
            System.out.println("- " + line.length);
            return;
        }
        String outputKey = line[11];
        double outputValue = Double.parseDouble(line[8]);
		values.collect(new Text(outputKey), new DoubleWritable(outputValue));
	}

}
