package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

public class MsmCrncTable {
	private static final Logger LOGGER = LogManager.getLogger(MsmCrncTable.class);
	
	private Table crncTable;
	private IndexCursor crncCursor;
		
	// Constructor
    public MsmCrncTable(Database mnyDb) throws IOException {
		crncTable = mnyDb.getTable("CRNC");
		crncCursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		return;
    }    
    
    /** 
     * Get the hcrncs of currencies by ISO code.
     * 
     * @param	isoCodes	the ISO codes to be found
     * @return				the corresponding hcrncs
     */
    public int[] getHcrncs(String[] isoCodes) throws IOException {
        Map<String, Object> row = null;
        Map<String, Object> rowPattern = new HashMap<>();
        int[] hcrncs = new int[isoCodes.length];
        for (int n = 0; n < isoCodes.length; n++) {
           	rowPattern.put("szIsoCode", isoCodes[n]);
            if (crncCursor.findFirstRow(rowPattern)) {
                row = crncCursor.getCurrentRow();
                hcrncs[n] = (int) row.get("hcrnc");
                LOGGER.info("Found currency {}, hcrnc = {}", isoCodes[n], hcrncs[n]);
            } else {
            	hcrncs[n] = 0;
            	LOGGER.warn("Cannot find currency {}", isoCodes[n]);
            }
        }
        return hcrncs;
    }

	/** 
	 * Get the ISO codes of all currencies that have the online update flag set.
	 * The base currency is returned as the last code in the list. 
	 * 
	 * @param	defHcrnc	the hcrnc of the default currency
	 * @return				the ISO codes
	 */
	public List<String> getIsoCodes(int defHcrnc) throws IOException {
	    Map<String, Object> row = null;
	    Map<String, Object> rowPattern = new HashMap<>();
	    Iterator<Row> crncIt;
	    String defIsoCode = null;
	    List<String> isoCodes = new ArrayList<>();
	    rowPattern.put("fOnline", true);
	    rowPattern.put("fHidden", false);
    	crncIt = new IterableBuilder(crncCursor).setMatchPattern(rowPattern).forward().iterator();
    	while (crncIt.hasNext()) {
    		row = crncIt.next();
    		if ((int) row.get("hcrnc") == defHcrnc) {
    			defIsoCode = (String) row.get("szIsoCode");
    			LOGGER.info("Base currency is {}, hcrnc = {}", defIsoCode, defHcrnc);
    		} else {
    			isoCodes.add((String) row.get("szIsoCode"));
    		}
    	}
    	// Add the base currecy as the last ISO code in the list
    	isoCodes.add(defIsoCode);
    	return isoCodes;
	}	
}