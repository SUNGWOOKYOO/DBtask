package InvertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class InvertedPair {
	public Text doc;
	public IntWritable num;
	public InvertedPair(Text _doc, IntWritable _num) {
		doc = _doc;
		num = _num;
	}
	public InvertedPair() {
	}
	
	public void setdoc(Text _doc) {
		doc = _doc;
	}
	public void setnum(IntWritable _num) {
		num = _num;
	}
	public Text getdoc() {
		return doc;
	}
	public IntWritable getnum() {
		return num;
	}
	
	@Override
	public String toString() {
		StringBuilder Info = new StringBuilder(doc.toString()+":"+num.toString());
		return Info.toString();
	}
}
