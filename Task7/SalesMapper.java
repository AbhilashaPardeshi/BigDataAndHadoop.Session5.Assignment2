package Task7;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Task7.Sales.myCounters;


/*
 * @author abhilasha
 * Input key is the offset of line in file
 * Input value format : CompanyName|ProductName|SizeInInches|State|PinCOde|Prize
 * Output key is the the name of the company+Product+Size
 * Output value is 1
 */
public class SalesMapper extends Mapper<LongWritable,Text,Television,LongWritable>
{
	private final static String DELIMITER = "|";
	private final static String NA = "NA";
	private final static LongWritable ONE  = new LongWritable(1);
	Television television;
	
	@Override
	public void setup(Context context)
	{
		television  = new Television();
	}

	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String strValue  = value.toString();
		System.out.println("Current line is "+strValue);
		
		StringTokenizer tokenizer  = new StringTokenizer(strValue, DELIMITER);
		String strCompanyName = tokenizer.nextToken();
		String strProductName  = tokenizer.nextToken();
		String strSize = tokenizer.nextToken();

		
		//Check if CompanyName or Product Name is 'NA'
		if(!strCompanyName.equalsIgnoreCase(NA)  && !strProductName.equalsIgnoreCase(NA))
		{
			television.set(strCompanyName,strSize);
			context.write(television,ONE);
			System.out.println("Mapper : Inserted "+television);
			context.getCounter(myCounters.VALIDRECORDS).increment(1);
		}
		else
		{
			context.getCounter(myCounters.INVALIDRECORDS).increment(1);
		}
	}
}
