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

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

class MsmCurrency extends MsmInstrument {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(MsmCurrency.class);
	private static final String PROPS_FILE = "MsmCurrency.properties";
	private static final String CRNC_TABLE = "CRNC";
	private static final String FX_TABLE = "CRNC_EXCHG";

	// Instance variables
	private final Table crncTable;
	private final Table fxTable;

	// Constructor
	MsmCurrency(Database msmDb) throws IOException {
		super(PROPS_FILE);

		// Open the currency tables
		crncTable = msmDb.getTable(CRNC_TABLE);
		fxTable = msmDb.getTable(FX_TABLE);
		return;
	}

	/**
	 * Updates the exchange rate for a currency pair.
	 * 
	 * @param quoteRow a row containing the currency quote data to update
	 * @return 0 update OK; 1 update with warnings; 2 update with errors
	 * @throws IOException
	 */
	int update(Map<String, String> sourceRow) throws IOException {

		// Validate incoming row and process status
		Map<String, Object> msmRow = new HashMap<>(buildMsmRow(sourceRow));
		int updateStatus = (int) msmRow.get("xStatus");
		String quoteType = msmRow.get("xType").toString();
		if (updateStatus == UPDATE_ERROR) {
			incSummary(quoteType, updateStatus);
			return updateStatus;
		}

		String symbol = msmRow.get("xSymbol").toString();
		LOGGER.info("Updating exchange rate for symbol {}", symbol);

		// Get hcrncs of currency pair
		int[] hcrnc = { 0, 0 };
		hcrnc[0] = getHcrnc(symbol.substring(0, 3));
		hcrnc[1] = getHcrnc(symbol.substring(3, 6));

		// Update exchange rate
		double newRate = (double) msmRow.get("rate");
		Map<String, Object> fxRowPattern = new HashMap<>();
		IndexCursor fxCursor = CursorBuilder.createCursor(fxTable.getPrimaryKeyIndex());
		Map<String, Object> fxRow = null;
		double oldRate = 0;
		int i;
		for (i = 0; i < 2; i++) {
			fxRowPattern.put("hcrncFrom", hcrnc[i]);
			fxRowPattern.put("hcrncTo", hcrnc[(i + 1) % 2]);
			if (fxCursor.findFirstRow(fxRowPattern)) {
				fxRow = fxCursor.getCurrentRow();
				oldRate = (double) fxRow.get("rate");
				if (i == 1) {
					// Reversed rate
					newRate = 1 / newRate;
					msmRow.put("rate", newRate);
				}
				LOGGER.info("Found exchange rate: from hcrnc = {}, to hcrnc = {}", hcrnc[i], hcrnc[(i + 1) % 2]);
				if (oldRate != newRate) {
					// Merge quote row into FX row and write to FX table
					fxRow.putAll(msmRow);		// TODO Should fxRow be sanitised first?
					fxCursor.updateCurrentRowFromMap(fxRow);
					LOGGER.info("Updated exchange rate: previous rate = {}, new rate = {}", oldRate, newRate);
				} else {
					LOGGER.info("Skipped exchange rate update, rate has not changed: previous rate = {}, new rate = {}", oldRate, newRate);
				}
				break;
			}
		}

		if (i == 2) {
			updateStatus = UPDATE_ERROR;
			LOGGER.error("Cannot find previous exchange rate");
		}
		incSummary(quoteType, updateStatus);
		return updateStatus;
	}

	/**
	 * Gets the hcrnc of a currency from the ISO code.
	 * 
	 * @param isoCode the ISO code to be found
	 * @return the corresponding hcrnc, or -1 if not found
	 * @throws IOException
	 */
	int getHcrnc(String isoCode) throws IOException {
		int hcrnc = -1;
		IndexCursor cursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("szIsoCode", isoCode));
		if (found) {
			hcrnc = (int) cursor.getCurrentRowValue(crncTable.getColumn("hcrnc"));
			LOGGER.info("Found currency {}, hcrnc = {}", isoCode, hcrnc);
		} else {
			LOGGER.warn("Cannot find currency {}", isoCode);
		}
		return hcrnc;
	}

	/**
	 * Gets the ISO codes of all currencies that have the online update flag set.
	 * 
	 * The base currency is returned as the last code in the list.
	 * 
	 * @param defHcrnc the hcrnc of the default currency
	 * @return the ISO codes
	 * @throws IOException
	 */
	List<String> getIsoCodes(int defHcrnc) throws IOException {
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