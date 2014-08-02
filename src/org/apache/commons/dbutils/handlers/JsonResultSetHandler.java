package org.apache.commons.dbutils.handlers;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.processors.JsonRowProcessor;
import org.apache.commons.dbutils.processors.RowProcessor;




public class JsonResultSetHandler implements ResultSetHandler<CharSequence> {
	
	private RowProcessor<CharSequence> convert = null;
	
	public JsonResultSetHandler(){
		this(JsonRowProcessor.ROW_PROCESSOR);
	}
	
	public JsonResultSetHandler(RowProcessor<CharSequence> convert){
		this.convert = convert;
	}

	public CharSequence handle(ResultSet rs) throws SQLException {
		StringBuffer results = new StringBuffer();

		results.append("[");

		while (rs.next()){
			results.append(convert.handleRow(rs))
			.append(",");
		} ;
		
		int len = results.length();
		if(len > 1){
			results.setLength(len-1);
		}
		
		results.append("]");

		return results;
	}
	
}
