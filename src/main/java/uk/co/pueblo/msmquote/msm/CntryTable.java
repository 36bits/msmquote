package uk.co.pueblo.msmquote.msm;

import java.io.IOException;
import java.util.Collections;

import com.healthmarketscience.jackcess.Database;

public class CntryTable extends MsmTable {

	// Constructor
	public CntryTable(Database msmDb) throws IOException {
		super(msmDb, "CNTRY");
		return;
	}    

	/** 
	 * Get the country code for the given hcntry.
	 * 
	 * @param	hcntry
	 * @return				the country code or null if not found
	 */
	public String getCode(int hcntry) throws IOException {
		boolean found = msmCursor.findFirstRow(Collections.singletonMap("hcntry", hcntry));
		if (found) {
			return (String) msmCursor.getCurrentRowValue(msmTable.getColumn("szCode"));
		}
		return null;
	}
}