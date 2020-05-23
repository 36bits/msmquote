package uk.co.pueblo.msmquote.msm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;

public class FxTable {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(FxTable.class);

	// Instance variables
	private Table fxTable;
	private IndexCursor fxCursor;

	// Constructor
	public FxTable(Database mnyDb) throws IOException {
		fxTable = mnyDb.getTable("CRNC_EXCHG");
		fxCursor = CursorBuilder.createCursor(fxTable.getPrimaryKeyIndex());
	}

	/**
	 * @param hcrncs
	 * @param newRate
	 * @return
	 * @throws IOException
	 */
	public boolean update(int[] hcrncs, double newRate) throws IOException {
		Map<String, Object> rateRowPattern = new HashMap<>();
		Column rateCol = fxTable.getColumn("rate");
		double oldRate = 0;
		for (int n = 0; n < 3; n++) {
			if (n == 2) {
				LOGGER.warn("Cannot find previous exchange rate");
				return false;
			}
			rateRowPattern.put("hcrncFrom", hcrncs[n]);
			rateRowPattern.put("hcrncTo", hcrncs[(n + 1) % 2]);
			if (fxCursor.findFirstRow(rateRowPattern)) {
				oldRate = (double) fxCursor.getCurrentRowValue(rateCol);
				if (n == 1) {
					// Reversed rate
					newRate = 1 / newRate;                	
				}
				LOGGER.info("Found currency pair: from hcrnc = {}, to hcrnc = {}", hcrncs[n], hcrncs[(n + 1) % 2]);
				if (oldRate != newRate) {
					fxCursor.setCurrentRowValue(rateCol, newRate);
					LOGGER.info("Updated exchange rate: previous rate = {}, new rate = {}", oldRate, newRate);
				} else {
					LOGGER.info("Skipped exchange rate update, rate has not changed: previous rate = {}, new rate = {}", oldRate, newRate);
				}
				return true;
			}	
		}
		return true;
	}
}