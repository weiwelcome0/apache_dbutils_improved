package org.apache.commons.dbutils.processors;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.RowProcessor;


public class CSVRowProcessor implements RowProcessor<CharSequence> {

	public static final RowProcessor<CharSequence> ROW_PROCESSOR= new CSVRowProcessor();
	
	public static final String lineStr = "\r\n";
	private String splitStr = null;
	
	
	public String getSplitStr() {
		return splitStr;
	}
	

	public CSVRowProcessor(){
		this(",");
	}
	

	public CSVRowProcessor(String splitStr){
		this.splitStr = splitStr;
	}
	
	
	@Override
	public CharSequence handleRow(ResultSet rs) throws SQLException {
		StringBuilder buf = new StringBuilder();

		int count = rs.getMetaData().getColumnCount();
		for(int i = 1; i <= count; i++){

			String value = rs.getString(i);
			if(null != value){
				buf.append(value);
			}			
			
			if(i != count){
				buf.append(splitStr);
			}
		}
		
		buf.append(lineStr);

		return buf;
		
	}

}
