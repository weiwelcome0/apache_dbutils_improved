package org.apache.commons.dbutils.handlers;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.processors.CSVRowProcessor;



public class CSVResultSetHandler implements ResultSetHandler<CharSequence> {

	private static final String lineStr = "\r\n";
	private String splitStr = null;
	
	public CSVResultSetHandler(){
		this(",");
	}
	
	public CSVResultSetHandler(String splitStr){
		this.splitStr = splitStr;
	}	
	
	@Override
	public CharSequence handle(ResultSet rs) throws SQLException {
		StringBuffer results = new StringBuffer();
		
		ResultSetMetaData rsmd = rs.getMetaData();
		results.append(getHeader(rsmd));
		
		while(rs.next()){
			results.append(handleRow(rs));
		};
		
		return results;
	}

	private CharSequence getHeader(ResultSetMetaData rsmd) throws SQLException {
		StringBuilder buf = new StringBuilder();
		
		int cols = rsmd.getColumnCount();
		for(int col = 1; col <= cols; col ++){
			
			String columnName = rsmd.getColumnLabel(col);
			if (null == columnName || 0 == columnName.length()) {
				columnName = rsmd.getColumnName(col);
			}

			buf.append(columnName).append(splitStr);   
		}
		
		int len = splitStr.length();
		buf.delete(buf.length()-len, len).append(CSVRowProcessor.lineStr);
		
		return buf;
	}
	
	public CharSequence handleRow(ResultSet rs) throws SQLException {
		StringBuilder buf = new StringBuilder();

		int count = rs.getMetaData().getColumnCount();
		for(int i = 1; i <= count; i++){

			String value = rs.getString(i);
			if(null != value){
				buf.append(value);
			}			
			
			buf.append(i < count?splitStr:lineStr);			
		}
		
		return buf;
	}
	
}
