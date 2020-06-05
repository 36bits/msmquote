package uk.co.pueblo.msmquote.msm;

import java.io.IOException;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;

public abstract class MsmTable {
		
	// Instance variables
	Table msmTable;
	IndexCursor msmCursor;

	/**
	 * Constructor
	 * 
	 * @param	db
	 * @param	table
	 * @throws IOException
	 */
	public MsmTable(Database db, String table) throws IOException {
		msmTable = db.getTable(table);
		msmCursor = CursorBuilder.createCursor(msmTable.getPrimaryKeyIndex());
	}
}
