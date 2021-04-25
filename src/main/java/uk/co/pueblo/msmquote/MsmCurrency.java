package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

public class MsmCurrency {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmCurrency.class);
	private static final String CRNC_TABLE = "CRNC";
	private static final String FX_TABLE = "CRNC_EXCHG";
	
	// Instance variables
	private Table crncTable;
	private Table fxTable;
	
	// Constructor
	public MsmCurrency(Database msmDb) throws IOException {
		// Open the currency tables
		crncTable = msmDb.getTable(CRNC_TABLE);		
		fxTable = msmDb.getTable(FX_TABLE);
		return;
	}    

	/** 
	 * Get the hcrncs of currencies by ISO code.
	 * 
	 * @param	isoCodes	the ISO codes to be found
	 * @return				the corresponding hcrncs
	 * @throws	IOException
	 */
	public int[] getHcrncs(String[] isoCodes) throws IOException {
		int[] hcrncs = new int[isoCodes.length];
		IndexCursor cursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		for (int n = 0; n < isoCodes.length; n++) {
			boolean found = cursor.findFirstRow(Collections.singletonMap("szIsoCode", isoCodes[n]));
			if (found) {
				hcrncs[n] = (int) cursor.getCurrentRowValue(crncTable.getColumn("hcrnc"));
				LOGGER.info("Found currency {}, hcrnc = {}", isoCodes[n], hcrncs[n]);
			} else {
				hcrncs[n] = 0;
				LOGGER.warn("Cannot find currency {}", isoCodes[n]);
			}
		}
		return hcrncs;
	}
	
	/**
	 * Updates the exchange rate for a currency pair. 
	 * 
	 * @param	hcrncs		the hcrncs of the two currencies
	 * @param	newRate		the new exchange rate
	 * @return				true if successful, otherwise false
	 * @throws	IOException
	 */
	boolean update(int[] hcrncs, double newRate) throws IOException {
		Map<String, Object> rateRowPattern = new HashMap<>();
		Column rateCol = fxTable.getColumn("rate");
		IndexCursor cursor = CursorBuilder.createCursor(fxTable.getPrimaryKeyIndex());
		double oldRate = 0;
		int n = 0;
		while (true) {
			if (n == 2) {
				LOGGER.warn("Cannot find previous exchange rate");
				return false;
			}
			rateRowPattern.put("hcrncFrom", hcrncs[n]);
			rateRowPattern.put("hcrncTo", hcrncs[(n + 1) % 2]);
			if (cursor.findFirstRow(rateRowPattern)) {
				oldRate = (double) cursor.getCurrentRowValue(rateCol);
				if (n == 1) {
					// Reversed rate
					newRate = 1 / newRate;                	
				}
				LOGGER.info("Found currency pair: from hcrnc = {}, to hcrnc = {}", hcrncs[n], hcrncs[(n + 1) % 2]);
				if (oldRate != newRate) {
					cursor.setCurrentRowValue(rateCol, newRate);
					LOGGER.info("Updated exchange rate: previous rate = {}, new rate = {}", oldRate, newRate);
				} else {
					LOGGER.info("Skipped exchange rate update, rate has not changed: previous rate = {}, new rate = {}", oldRate, newRate);
				}
				break;
			}
			n++;
		}
		return true;
	}

	/** 
	 * Get the ISO codes of all currencies that have the online update flag set.
	 * 
	 * The base currency is returned as the last code in the list. 
	 * 
	 * @param	defHcrnc	the hcrnc of the default currency
	 * @return				the ISO codes
	 * @throws	IOException
	 */
	public List<String> getIsoCodes(int defHcrnc) throws IOException {
		Map<String, Object> row = null;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> crncIt;
		String defIsoCode = null;
		List<String> isoCodes = new ArrayList<>();
		rowPattern.put("fOnline", true);
		rowPattern.put("fHidden", false);
		IndexCursor cursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		crncIt = new IterableBuilder(cursor).setMatchPattern(rowPattern).forward().iterator();
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