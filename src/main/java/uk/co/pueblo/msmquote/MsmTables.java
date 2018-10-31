package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

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
	
	private ArrayList<Map<String, Object>> spRowList = new ArrayList<>();
	IterableBuilder spItBuilder;
	private int hsp = 0;
		
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
				
		spItBuilder = new IterableBuilder(spCursor);
		
		// Get current hsp (SP table index)
		// TODO Use index to get current hsp
		int rowHsp = 0;
		Column column = spTable.getColumn("hsp");
		spCursor.beforeFirst();
		while (spCursor.moveToNextRow()) {
			hsp = (int) spCursor.getCurrentRowValue(column);
			if (rowHsp > hsp) {
		 		hsp = rowHsp;
		 	}
		 }
	}
	
	public boolean update(Map<String, Object> quoteRow) throws IOException  {
		
		if (quoteRow.get("rate") != null) {
			// Must be an FX quote
			return updateFxRow(quoteRow);
		} else {		
			return updateQuoteRows(quoteRow);
		}
	}

	public void addNewSpRows() throws IOException{
		if (!spRowList.isEmpty()) {
			LOGGER.info("Adding new quotes to SP table");
			spTable.addRowsFromMaps(spRowList);
		}
		return;
	}
	
	private boolean updateQuoteRows(Map<String, Object> quoteRow) throws IOException {
		
		Map<String, Object> row = null;
		String symbol = (String) quoteRow.get("szSymbol");
		int hsec;
		
		// Update SEC table
		if ((row = getSecRowToUpdate(symbol)) == null) {
			LOGGER.warn("Cannot find symbol " + symbol + " in SEC table");
			return false;
		} else {
			hsec = (int) row.get("hsec");
			LOGGER.info("Found symbol " + symbol + " in SEC table: sct = " + row.get("sct") + ", hsec = " + hsec);
	    	// Merge quote row into SEC row and write to SEC table
	    	row.putAll(quoteRow);
	    	row.put("dtSerial", new Date());		    	// dtSerial is assumed to be record creation/update time-stamp
	    	secCursor.updateCurrentRowFromMap(row);
	        LOGGER.info("Updated quote in SEC table for symbol " + symbol);
		}
		
		// Update SP table
		Date quoteDate = (Date) quoteRow.get("dt");
		if ((row = getSpRowToUpdate(hsec, quoteDate)) != null) {
			LOGGER.info("Found previous quote to update in SP table for symbol " + symbol + ": " + row.get("dt") + ", price = " + row.get("dPrice") + ", hsp = " + row.get("hsp"));
	    	// Merge quote row into SP row and write to SP table
       		row.putAll(quoteRow);
	    	row.put("dtSerial", new Date());		    	// dtSerial is assumed to be record creation/update time-stamp
       		spCursor.updateCurrentRowFromMap(row);
            LOGGER.info("Updated previous quote in SP table for symbol " + symbol + ": " + row.get("dt") + ", new price = " + row.get("dPrice"));
		} else {
			if ((row = getSpRowToCopy(hsec, quoteDate)) == null) {
				LOGGER.info("Cannot find previous quote in SP table for symbol " + symbol);
				row = quoteRow;
	    	   	row.put("hsec", hsec);
			} else {
				LOGGER.info("Found previous quote in SP table for symbol " + symbol + ": " + row.get("dt") + ", price = " + row.get("dPrice") + ", hsp = " + row.get("hsp"));
				row.putAll(quoteRow);
			}
			hsp = hsp + 1;
			row.put("hsp", hsp);
		   	row.put("src", ONLINE);
		   	row.put("dtSerial", new Date());		    	// dtSerial is assumed to be record creation/update time-stamp
	    	//spTable.addRowFromMap(row);
		   	spRowList.add(row);
	       	LOGGER.info("Added new quote to SP table update for symbol " + symbol + ": " + row.get("dt") + ", new price = " + row.get("dPrice") + ", new hsp = " + row.get("hsp"));
		}				
		
		return true;
	}		
	
    /** 
     * Searches the SEC table for the row matching the supplied symbol.
     * 
     * @param	symbol	symbol to find
     * @return	SEC row if found or null if not found
     */
    private Map<String, Object> getSecRowToUpdate(String symbol) throws IOException {
    	Map<String, Object> returnRow = null;
        Map<String, Object> rowPattern = new HashMap<>();
		
		// Find matching symbol in SEC table
    	rowPattern.put("szSymbol", symbol);
    	if (secCursor.findFirstRow(rowPattern)) {
    		returnRow = secCursor.getCurrentRow();
	   	}
    	return returnRow;
    }
            
    /** 
     * Searches the SP table for a row matching the supplied hsec and date.
     *
     * @param	hsec	hsec for search
     * @param	date	date for search
     * @return	SP row if match found or null if not found
     */
    private Map<String, Object> getSpRowToUpdate(int hsec, Date date) throws IOException {
    	Map<String, Object> rowPattern = new HashMap<>();
    
        rowPattern.put("hsec", hsec);
    	rowPattern.put("dt", date);
    	int[] srcs = { ONLINE, MANUAL };
    	for (int src : srcs) {
        	rowPattern.put("src", src);
               	if (spCursor.findFirstRow(rowPattern)) {
               		return spCursor.getCurrentRow();
	           	}
    	}
    	return null;
    }
    
    /** 
     * Searches the SP table for a row matching the supplied hsec and
     * the most recent date previous to the supplied date.
     *
     * @param	hsec	hsec for search
     * @param	date	date for search
     * @return	SP row if match found or null if not found
     */
    private Map<String, Object> getSpRowToCopy(int hsec, Date date) throws IOException {
    	Map<String, Object> row = null;
    	Map<String, Object> returnRow = null;
    	Map<String, Object> rowPattern = new HashMap<>();
    	Date rowDate = new Date(0);
    	Date maxDate = new Date(0);
    	
    	rowPattern.put("hsec", hsec);
    	Iterator<Row> spIt = spItBuilder.setMatchPattern(rowPattern).iterator();
    	while (spIt.hasNext()) {
    		row = spIt.next();
    		int src = (int) row.get("src");
			rowDate = (Date) row.get("dt");
			if (rowDate.after(maxDate)) {
				// Test for previous manual or online quote
				if ((src == ONLINE || src == MANUAL) && rowDate.before(date)) {
		        	maxDate = rowDate;
		        	returnRow = row;
			    }
				// Test for previous buy
		        if (src == BUY && (rowDate.before(date) || rowDate.equals(date))) {
	        		maxDate = rowDate;
	        		returnRow = row;
	        	}
			}
	   	}
    	return returnRow;
    }	
    
    private boolean updateFxRow(Map<String, Object> quoteRow) throws IOException {
    	
    	String currencyPair = (String) quoteRow.get("symbol");
    	
		LOGGER.info("Processing quote for currency pair " + currencyPair);
            	
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
                LOGGER.info("Found currency " + isoCode + ", hcrnc = " + hcrncs[n]);
            } else {
            	LOGGER.warn("Cannot find currency " + isoCode);
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
                rateCursor.setCurrentRowValue(column, newRate);
                LOGGER.info("Updated currency pair " + currencyPair + ": previous rate = " + oldRate + ", new rate = " + newRate);
                return true;
            }	
        }
        return true;
    }
    
}