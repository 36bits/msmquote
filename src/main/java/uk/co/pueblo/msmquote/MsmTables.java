package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Table;

public class MsmTables {
	private static final Logger LOGGER = Logger.getLogger(MsmTables.class);
	
	private Table secTable;
	private Table spTable;
	private Table crncTable;
	private Table rateTable;
	private IndexCursor secCursor;
	private IndexCursor spCursor;
	private IndexCursor crncCursor;
	private IndexCursor rateCursor;
		
	// Set SP table src constants
    private static final int BUY = 1;
    private static final int MANUAL = 5;
    private static final int ONLINE = 6;
	
	// Constructor
	public MsmTables(Database mnyDb) throws IOException {
		 secTable = mnyDb.getTable("SEC");
		 spTable = mnyDb.getTable("SP");
		 crncTable = mnyDb.getTable("CRNC");
		 rateTable = mnyDb.getTable("CRNC_EXCHG");
		 secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
		 spCursor = CursorBuilder.createCursor(spTable.getPrimaryKeyIndex());
		 crncCursor = CursorBuilder.createCursor(crncTable.getPrimaryKeyIndex());
		 rateCursor = CursorBuilder.createCursor(rateTable.getPrimaryKeyIndex());
	}
	
	public boolean update(Map<String, Object> quoteRow) throws IOException  {
		
		if (quoteRow.get("rate") != null) {
			// Must be an FX quote
			return updateFxRow(quoteRow);
		} else {		
			return updateQuoteRows(quoteRow);
		}
	}

	private boolean updateQuoteRows(Map<String, Object> quoteRow) throws IOException {
		
		Map<String, Object> row = null;
        Map<String, Object> rowPattern = new HashMap<String, Object>();
		
		// Find symbol in SEC table
    	int hsec = -1;
		String symbol = (String) quoteRow.get("szSymbol");
        rowPattern.put("szSymbol", symbol);
    	if (secCursor.findFirstRow(rowPattern)) {
            row = secCursor.getCurrentRow();
            hsec = (int) row.get("hsec");
        }
    		
    	if (hsec == -1) {
    		LOGGER.warn("Cannot find symbol " + symbol + " in SEC table");
    		return false;
	   	}

    	LOGGER.info("Found symbol " + symbol + " in SEC table: sct = " + row.get("sct") + ", hsec = " + hsec);
    	
    	// Merge quote row and SEC row into update row
    	Map<String, Object> updateRow = new HashMap<String, Object>();
    	updateRow.putAll(row);
    	updateRow.putAll(quoteRow);
    	    	
    	// Write update row to SEC table
    	// dtSerial is assumed to be record creation/update time-stamp
    	updateRow.put("dtSerial", new Date());
    	secCursor.updateCurrentRowFromMap(updateRow);
        LOGGER.info("Updated quote data in SEC table for symbol " + symbol);
        
        // Clear maps ready for SP table update
        row.clear();
        rowPattern.clear();
        updateRow.clear();
        		
		// Find matching hsec and quote date in SP table
        boolean needNewSpRow = true;
    	Date newDate = (Date) quoteRow.get("dt");
    	rowPattern.put("hsec", hsec);
    	rowPattern.put("dt", newDate);
    	double oldPrice = 0;
    	Date oldDate = null;
    	int[] srcs = { MANUAL, ONLINE };

    	for (int src : srcs) {
        	rowPattern.put("src", src);
               	if (spCursor.findFirstRow(rowPattern)) {
               		// Matching SP row found
               		row = spCursor.getCurrentRow();
               		oldPrice = (double) row.get("dPrice");
               		oldDate = (Date) row.get("dt");
               		needNewSpRow = false;
               		break;
        	}
    	}
       	if (needNewSpRow) {
    		// No matching SP row found so get a new row
    		row = getNewSpRow(spCursor, hsec, newDate);
    		oldDate = (Date) row.get("dt");
    		oldPrice = (double) row.get("dPrice");
    		// Index autonumber test:
    		// spRow.put("hsp", null);
    		row.put("dt", newDate);
    		row.put("src", ONLINE);
       	}
       	       			
       	// getNewSpRow sets dPrice to -1 if no previous quote was found
       	if (oldPrice == -1) {
       		LOGGER.info("Cannot find previous quote in SP table for symbol " + symbol);
       	}
       	else {
       		LOGGER.info("Found previous quote in SP table for symbol " + symbol + ": " + oldDate + ", price = " + oldPrice);
       	}
       	       	       	
    	// Merge quote row and SP row into update row
       	updateRow.putAll(row);
    	updateRow.putAll(quoteRow);
    	    	
    	// Write update row to SP table
    	// dtSerial is assumed to be record creation/update time-stamp
    	updateRow.put("dtSerial", new Date());
    	if (needNewSpRow) {
        	spTable.addRowFromMap(updateRow);
           	LOGGER.info("Added new quote data in SP table for symbol " + symbol + ": " + newDate + ", new price = " + updateRow.get("dPrice") + ", new hsp = " + updateRow.get("hsp"));
        } else {
        	spCursor.updateCurrentRowFromMap(updateRow);
            LOGGER.info("Updated previous quote data in SP table for symbol " + symbol + ": " + newDate + ", new price = " + updateRow.get("dPrice") + ", hsp = " + updateRow.get("hsp"));
    	}   
       	     	    	
		return true;
	}
	
	/** 
     * Build a new SP table row.
     * 
     * Returns a row containing the next hsp (index) and the most recent previous quote data.
     * 
     */
    private Map<String, Object> getNewSpRow(Cursor cursor, int hsec, Date quoteDate) throws IOException {
    	Map<String, Object> spRow = null;
    	Map<String, Object> newSpRow = new HashMap<String, Object>();
    	int rowHsp = 0;
    	int maxHsp = 0;
    	Date rowDate = new Date(0);
    	Date maxDate = new Date(0);
    	
    	// Build initial minimal new row
    	newSpRow.put("hsec", hsec);
    	newSpRow.put("dPrice", (double) -1);
    	    	
    	// Find highest hsp and any previous quotes
       	cursor.beforeFirst();
    	while (true) {
    		spRow = cursor.getNextRow();
    		if (spRow == null) {
    			break;
    		}
    		
    		// Update highest hsp seen
    		rowHsp = (int) spRow.get("hsp");
    		if (rowHsp > maxHsp) {
    			maxHsp = rowHsp;
    		}
    		
			int src = (int) spRow.get("src");
			rowDate = (Date) spRow.get("dt");
			if (((int) spRow.get("hsec") == hsec) && rowDate.after(maxDate)) {
				// Test for previous manual or online quote
				if ((src == MANUAL || src == ONLINE) && rowDate.before(quoteDate)) {
		        	maxDate = rowDate;
		        	newSpRow = spRow;
			    }
				// Test for previous buy
		        if (src == BUY && (rowDate.before(quoteDate) || rowDate.equals(quoteDate))) {
	        		maxDate = rowDate;
		        	newSpRow = spRow;
	        	}
	        }
    	}
			
    newSpRow.put("hsp", maxHsp + 1);
	return newSpRow;
    }
    
    private boolean updateFxRow(Map<String, Object> quoteRow) throws IOException {
    	
    	String currencyPair = (String) quoteRow.get("symbol");
    	
		LOGGER.info("Processing quote for currency pair " + currencyPair);
            	
        // Find currencies in CRNC table
        Map<String, Object> crncRow = null;
        Map<String, Object> crncRowPattern = new HashMap<String, Object>();
        String isoCode = null;
        int[] hcrncs = {0, 0};
        int n = 0;    
        for (n = 0; n < 2; n++) {
        	isoCode = currencyPair.substring(n * 3, (n + 1) * 3); 
        	crncRowPattern.put("szIsoCode", isoCode);
            if (crncCursor.findFirstRow(crncRowPattern)) {
                crncRow = crncCursor.getCurrentRow();
                hcrncs[n] = (int) crncRow.get("hcrnc");
                LOGGER.info("Found currency " + isoCode + ", hcrnc = " + hcrncs[n]);
            } else {
            	LOGGER.warn("Cannot find currency " + isoCode);
        		return false;
    	   	}
        }	
                                      
        // Now find and update rate for currency pair in CRNC_EXCHG table
        Map<String, Object> rateRow = null;
        Map<String, Object> rateRowPattern = new HashMap<String, Object>();
        double oldRate = 0;
        double newRate = (double) quoteRow.get("rate");
        
        for (n = 0; n < 3; n++) {
        	if (n == 2) {
        		LOGGER.warn("Cannot find previous rate for currency pair " + currencyPair);
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
                LOGGER.info("Found currency pair: from hcrnc = " + hcrncs[n] + ", to hcrnc = " + hcrncs[(n + 1) % 2]);
                Column column = rateTable.getColumn("rate");
                rateCursor.setCurrentRowValue(column, newRate);
                LOGGER.info("Updated currency pair " + currencyPair + ": previous rate = " + oldRate + ", new rate = " + newRate);
                return true;
            }	
        }
    return true;
    }
    
}
