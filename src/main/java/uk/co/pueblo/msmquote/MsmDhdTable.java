package uk.co.pueblo.msmquote;

import java.io.IOException;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

public class MsmDhdTable {
		private Table dhdTable;
			
	// Constructor
    public MsmDhdTable(Database mnyDb) throws IOException {
		dhdTable = mnyDb.getTable("DHD");
		return;
    }    
    
    /** 
     * Get the hcrnc of the base currency.
     * 
     * @return				the hcrnc
     */
    public int getDefHcrnc() throws IOException {
    	Row row = dhdTable.getNextRow();
    	return (int) row.get("hcrncDef");        
    }
}