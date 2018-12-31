package task4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class hwMapper extends Mapper<Object, Text, Text, Text>{
	private Text key = new Text();
	private Text Attrs = new Text();
	
	// map function is called when it comes across each line 
	@Override
	protected void map(Object map_key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		System.out.println("=============================================================");
		System.out.println("			Map fuction ...			");
		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		StringTokenizer itr = new StringTokenizer(value.toString());
		// deal with one line
		ArrayList<String> InfoList = new ArrayList<>();
		while (itr.hasMoreTokens()) {
			InfoList.add(itr.nextToken());
		}
		Iterator<String> it = InfoList.iterator();
		String TableAndId;
		String Table = new String();
		String Id = new String();
		String Joinkey = new String();
		if(it.hasNext()) {
			TableAndId = it.next();
			Table = TableAndId.substring(0, 1);
			Id = TableAndId.substring(1, 2);
		}
		StringBuilder Attributes = new StringBuilder();
		Attributes.append(Table+" "+Id+" ");
		if(it.hasNext()) {
			Joinkey = it.next();
			Attributes.append(Joinkey);
		}
		while(it.hasNext()) {
			Attributes.append(it.next());
		}
		Attributes.append(";");
		Attrs.set(Attributes.toString());
		key.set(Joinkey);
		context.write(key, Attrs);
		System.out.println("=                                                           =");
		System.out.println("=    Context.write(key, Attrs): "+key+", "+Attrs+"		");
		System.out.println("=                                                           =");
		
		System.out.println("= = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =");
		System.out.println("			Map fuction END			");
		System.out.println("=============================================================");
		
	}
}
