package Task7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//CompanyName|ProductName|SizeInInches|State|PinCOde|Prize


/**
 * 
 * @author abhilasha
 * Bean class containing television info
 *
 */
public class Television implements WritableComparable<Television>
{
	private Text company;
//	private String strProduct;
	private FloatWritable size;

	public Text getCompany() 
	{
		return company;
	}
//	public String getProduct() 
//	{
//		return strProduct;
//	}
	public FloatWritable getSize() 
	{
		return size;
	}
	
	public void set(String strCompanyName,String strSize )
	{
		this.company = new Text(strCompanyName);
	//	this.strProduct = strProductName;
		this.size = new FloatWritable(Float.parseFloat(strSize));
		
	}

	
	//Deserialize the data
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		company = new Text(in.readUTF());
//		strProduct = in.readUTF();
		size = new FloatWritable(in.readFloat());
	}
	
	//Seralize data
	@Override
	public void write(DataOutput out) throws IOException 
	{
		out.writeUTF(company.toString());
	//	out.writeUTF(strProduct);
		out.writeFloat(size.get());
	}
	
	
	//Used for sorting the objects. Comparison made on basis of size
	@Override
	public int compareTo(Television o) 
	{
		int iCompareValue = this.getCompany().compareTo(o.getCompany());
		if (iCompareValue ==0 ) 
		{
			iCompareValue = (-1)*(this.getSize().compareTo(o.getSize()));
		}
		return iCompareValue;
	}
	@Override
	public String toString() {
		return "Television[company=" + company + ", size=" + size + "]";
	}

	
}
