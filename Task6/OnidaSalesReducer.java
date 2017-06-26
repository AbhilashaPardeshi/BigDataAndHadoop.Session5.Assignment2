package OnidaSales;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author abhilasha
 * Input key is the the name of the state
 * Input value is 1
 * Output key is the state name
 * OUtput value is the count of televisions of Onida in a particular state
 *
 */
public class OnidaSalesReducer extends Reducer<Text,LongWritable,Text,LongWritable>
{
	LongWritable count;
	
	@Override
	public void setup(Context context)
	{
		count = new LongWritable();
	}
	
	@Override
	public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
	{
		long lCount = 0;
		for(LongWritable value:values)
		{
			lCount=lCount + value.get();
		}
		
		count.set(lCount);
		context.write(key, count);
	}
}
