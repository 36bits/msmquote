package uk.co.pueblo.msmquote;

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

public class MsmFxTable {
	private static final Logger LOGGER = LogManager.getLogger(MsmFxTable.class);
	
	private Table crncTable;
	private Table rateTable;
	private IndexCursor crncCursor;
	private IndexCursor rateCursor;
	
	// Constructor
    public MsmFxTable(Database mnyDb) throws IOException {
		crncTable = mnyDb.getTable("CRNC");
		rateTable = mnyDb.getTable("CRNC_EXCHG");
		crncCursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		rateCursor = CursorBuilder.createCursor(rateTable.getPrimaryKeyIndex());
    }
    
public boolean update(Map<String, Object> quoteRow) throws IOException {
    	
    	String currencyPair = (String) quoteRow.get("symbol");    	
		LOGGER.info("Processing quote for currency pair {}", currencyPair);
            	
        // Find currencies in CRNC table
        Map<String, Object> crncRow = null;
        Map<String, Object> crncRowPattern = new HashMap<>();
        String isoCode = null;
        int[] hcrncs = {0, 0};
        int n = 0;    
        for (n = 0; n < 2; n++) {
        	isoCode = currencyPair.substring(n * 3, (n + 1) * 3); 
        	crncRowPattern.put("szIsoCode", isoCode);
            if (crncCursor.findFirstRow(crncRowPattern)) {
                crncRow = crncCursor.getCurrentRow();
                hcrncs[n] = (int) crncRow.get("hcrnc");
                LOGGER.info("Found currency {}, hcrnc = {}", isoCode, hcrncs[n]);
            } else {
            	LOGGER.warn("Cannot find currency {}", isoCode);
        		return false;
    	   	}
        }	
                                      
        // Now find and update rate for currency pair in CRNC_EXCHG table
        Map<String, Object> rateRow = null;
        Map<String, Object> rateRowPattern = new HashMap<>();
        Column column = rateTable.getColumn("rate");
        double oldRate = 0;
        double newRate = (double) quoteRow.get("rate");
        
        for (n = 0; n < 3; n++) {
        	if (n == 2) {
        		LOGGER.warn("Cannot find previous rate for currency pair {}", currencyPair);
        		return false;
        	}
           	rateRowPattern.put("hcrncFrom", hcrncs[n]);
            rateRowPattern.put("hcrncTo", hcrncs[(n + 1) % 2]);
            if (rateCursor.findFirstRow(rateRowPattern)) {
                rateRow = rateCursor.getCurrentRow();
                oldRate = (double) rateRow.get("rate");
                if (n == 1) {
                	// Reversed rate
                	newRate = 1 / newRate;                	
                }
                LOGGER.info("Found currency pair: from hcrnc = {}, to hcrnc = {}", hcrncs[n], hcrncs[(n + 1) % 2]);
                rateCursor.setCurrentRowValue(column, newRate);
                LOGGER.info("Updated currency pair {}: previous rate = {}, new rate = {}", currencyPair, oldRate, newRate);
                return true;
            }	
        }
        return true;
    }
    
    
}
