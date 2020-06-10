package uk.co.pueblo.msmquote.msm;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;

public class FxTable extends MsmTable {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(FxTable.class);

	// Constructor
	public FxTable(Database msmDb) throws IOException {
		super(msmDb, "CRNC_EXCHG");
	}

	/**
	 * Update the exchange rate for a currency pair. 
	 * 
	 * @param	hcrncs		the hcrncs of the two currencies
	 * @param	newRate		the new exchange rate
	 * @return				true if successful, otherwise false
	 * @throws	IOException
	 */
	public boolean update(int[] hcrncs, double newRate) throws IOException {
		Map<String, Object> rateRowPattern = new HashMap<>();
		Column rateCol = msmTable.getColumn("rate");
		double oldRate = 0;
		int n = 0;
		while (true) {
			if (n == 2) {
				LOGGER.warn("Cannot find previous exchange rate");
				return false;
			}
			rateRowPattern.put("hcrncFrom", hcrncs[n]);
			rateRowPattern.put("hcrncTo", hcrncs[(n + 1) % 2]);
			if (msmCursor.findFirstRow(rateRowPattern)) {
				oldRate = (double) msmCursor.getCurrentRowValue(rateCol);
				if (n == 1) {
					// Reversed rate
					newRate = 1 / newRate;                	
				}
				LOGGER.info("Found currency pair: from hcrnc = {}, to hcrnc = {}", hcrncs[n], hcrncs[(n + 1) % 2]);
				if (oldRate != newRate) {
					msmCursor.setCurrentRowValue(rateCol, newRate);
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
}