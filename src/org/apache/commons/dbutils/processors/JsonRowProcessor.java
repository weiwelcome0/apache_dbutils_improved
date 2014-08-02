package org.apache.commons.dbutils.processors;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


public class JsonRowProcessor implements RowProcessor<CharSequence> {
	
	public static final JsonRowProcessor ROW_PROCESSOR =  new JsonRowProcessor();
	
	private ResultSetMetaData cache_rsmd = null;		
	private String[] columnNames = null;
	
	@Override
	public CharSequence handleRow(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		String[] columnNames = this.getAllColumnName(rsmd);

		StringBuilder results  = this.rowToJson(rs, columnNames);
		
		return results;
	}
	
	/**
	 * get all column name of the resultset
	 * @param rsmd
	 * @return
	 * @throws SQLException
	 */
	protected String[] getAllColumnName(ResultSetMetaData rsmd) throws SQLException {
		if(!rsmd.equals(cache_rsmd)){
			int cols = rsmd.getColumnCount();
	        this.columnNames = new String[cols + 1];
	        
	        for (int col = 1; col <= cols; col++) {
	            String columnName = rsmd.getColumnLabel(col);
	            if (null == columnName || 0 == columnName.length()) {
	              columnName = rsmd.getColumnName(col);
	            }
	            columnNames[col] = columnName.toLowerCase();
	        }
		}
        
		return columnNames;
	}
	
	/**
	 * convert a resultset row to JSON String
	 * @param rs
	 * @param columnNames
	 * @return
	 * @throws SQLException
	 */
	protected StringBuilder rowToJson(ResultSet rs, String[] columnNames) throws SQLException{
		StringBuilder buf = new StringBuilder();
		
		buf.append("{");
		
		for(int i = 1; i < columnNames.length; i++){

			//assemble the name and value like "name":"value"
			String value = rs.getString(i);
			if(null != value){
				buf.append("\"")
				.append(columnNames[i])
				.append("\":\"")
				.append(value);
			}	
			
			if(i != columnNames.length - 1){
				buf.append("\",");
			}
		}
		
		buf.append("}");
		
		return buf;		
	}

}
