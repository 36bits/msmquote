package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.Collections;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;

public class MsmCntryTable {
	
	// Instance variables
	private Table cntryTable;
	private IndexCursor cntryCursor;		

	// Constructor
	public MsmCntryTable(Database mnyDb) throws IOException {
		cntryTable = mnyDb.getTable("CNTRY");
		cntryCursor = CursorBuilder.createCursor(cntryTable.getPrimaryKeyIndex());		
		return;
	}    

	/** 
	 * Get the country code for the given hcntry.
	 * 
	 * @param	hcntry
	 * @return				the country code or null if not found
	 */
	public String getCode(int hcntry) throws IOException {
		boolean found = cntryCursor.findFirstRow(Collections.singletonMap("hcntry", hcntry));
		if (found) {
			return (String) cntryCursor.getCurrentRowValue(cntryTable.getColumn("szCode"));
		}
		return null;
	}
}