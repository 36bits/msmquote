package uk.co.pueblo.msmquote.msm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.util.IterableBuilder;

public class CrncTable extends MsmTable{

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(CrncTable.class);

	// Constructor
	public CrncTable(Database msmDb) throws IOException {
		super(msmDb, "CRNC");
		
		return;
	}    

	/** 
	 * Get the hcrncs of currencies by ISO code.
	 * 
	 * @param	isoCodes	the ISO codes to be found
	 * @return				the corresponding hcrncs
	 */
	public int[] getHcrncs(String[] isoCodes) throws IOException {
		int[] hcrncs = new int[isoCodes.length];
		for (int n = 0; n < isoCodes.length; n++) {
			boolean found = msmCursor.findFirstRow(Collections.singletonMap("szIsoCode", isoCodes[n]));
			if (found) {
				hcrncs[n] = (int) msmCursor.getCurrentRowValue(msmTable.getColumn("hcrnc"));
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
		crncIt = new IterableBuilder(msmCursor).setMatchPattern(rowPattern).forward().iterator();
		while (crncIt.hasNext()) {
			row = crncIt.next();
			if ((int) row.get("hcrnc") == defHcrnc) {
				defIsoCode = (String) row.get("szIsoCode");
				LOGGER.info("Base currency is {}, hcrnc = {}", defIsoCode, defHcrnc);
			} else {
				isoCodes.add((String) row.get("szIsoCode"));
			}
		}
		// Add the base currency as the last ISO code in the list
		isoCodes.add(defIsoCode);
		return isoCodes;
	}	
}