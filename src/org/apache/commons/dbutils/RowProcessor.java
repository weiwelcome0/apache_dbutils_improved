package org.apache.commons.dbutils;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowProcessor<T> {
	
	T handleRow(ResultSet rs) throws SQLException;
	
}
