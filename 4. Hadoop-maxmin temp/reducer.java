package temp;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> value1, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException {
			int maxvalue=Integer.MIN_VALUE;
			while(value1.hasNext()){
				maxvalue=Math.max(maxvalue, value1.next().get());
			}
			values.collect(key, new IntWritable(maxvalue));
	}
}
