package earthquake;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		public void reduce(Text key, Iterator<DoubleWritable> value1, OutputCollector<Text, DoubleWritable> values, Reporter r) throws IOException {
            double maxMagnitude = Double.MIN_VALUE;
            while(value1.hasNext()){
                maxMagnitude = Math.max(maxMagnitude, value1.next().get());
			}
            values.collect(key, new DoubleWritable(maxMagnitude));
	}
}
