package org.apache.commons.dbutils.processors;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.commons.dbutils.RowProcessor;


public class ArrayRowProcessor implements RowProcessor<Object[]> {

	public static final ArrayRowProcessor ROW_PROCESSOR = new ArrayRowProcessor();
	
	@Override
	public Object[] handleRow(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        Object[] result = new Object[cols];

        for (int i = 0; i < cols; i++) {
            result[i] = rs.getObject(i + 1);
        }

        return result;
	}

}
