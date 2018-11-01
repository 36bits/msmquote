package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
		
		// Build SP row
		Date quoteDate = (Date) quoteRow.get("dt");
		if ((row = getSpRow(hsec, quoteDate)) == null) {
			LOGGER.info("Cannot find previous quote in SP table for symbol " + symbol);
			row = quoteRow;
    	   	row.put("hsec", hsec);
		} else {  
			if (row.containsKey("hsp")) {
				LOGGER.info("Found previous quote to update in SP table for symbol " + symbol + ": " + row.get("dt") + ", price = " + row.get("dPrice") + ", hsp = " + row.get("hsp"));
			} else {
				LOGGER.info("Found previous quote in SP table for symbol " + symbol + ": " + row.get("dt") + ", price = " + row.get("dPrice") + ", hsp = " + row.get("hsp"));
			}
	    	// Merge quote row into SP row
       		row.putAll(quoteRow);
		}
			
		// Update SP row
		row.put("dtSerial", new Date());	// dtSerial is assumed to be record creation/update time-stamp
		if (row.containsKey("hsp")) {
			spCursor.updateCurrentRowFromMap(row);
            LOGGER.info("Updated previous quote in SP table for symbol " + symbol + ": " + row.get("dt") + ", new price = " + row.get("dPrice"));
		} else {
			hsp = hsp + 1;
			row.put("hsp", hsp);
		   	row.put("src", ONLINE);
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
     * @return	SP row if match for quote date found; hsp key is removed if row is for reference only; null if no row for hsec found
     */
    private Map<String, Object> getSpRow(int hsec, Date quoteDate) throws IOException {
    	
    	Map<String, Object> row;
    	Map<String, Object> rowPattern = new HashMap<>();
    	Iterator<Row> spIt;
    	Instant firstInstant;
    	Instant lastInstant;
    	
    	rowPattern.put("hsec", hsec);
    	
		// Get instants for date from first and last rows for this hsec 
    	spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).forward().iterator();
    	if (!spIt.hasNext()) {
    		return null;	// No rows in SP table for this hsec
    	} else {
    		row = spIt.next();
    		firstInstant = ((Date) row.get("dt")).toInstant();
    		spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).reverse().iterator();
    		row = spIt.next();
        	lastInstant = ((Date) row.get("dt")).toInstant();
    	}
    	    	    	
    	// Build iterator with the closest date to the quote date
    	Instant quoteInstant = quoteDate.toInstant();
    	long firstDays = Math.abs(ChronoUnit.DAYS.between(firstInstant, quoteInstant));
    	long lastDays = Math.abs(ChronoUnit.DAYS.between(lastInstant, quoteInstant));
    	LOGGER.debug("Instants: first = " + firstInstant + ", last = " + lastInstant + ", quote = " + quoteInstant);
    	LOGGER.debug("Days: first->quote = " + firstDays + ", last->quote = " + lastDays);
    	
    	if (lastDays < firstDays) {
    		spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).reverse().iterator();
    	} else {
    		spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).forward().iterator();
    	}
    	    	
    	Map<String, Object> refQuoteRow = null;
    	Instant rowInstant = Instant.ofEpochMilli(0);
    	Instant maxInstant = Instant.ofEpochMilli(0);
    	    	
    	while (spIt.hasNext()) {
    		row = spIt.next();
    		LOGGER.debug(row);
			if ((rowInstant = ((Date) row.get("dt")).toInstant()).isBefore(maxInstant)) {
				continue;
			}
			int src = (int) row.get("src");
			if ((src == ONLINE || src == MANUAL) && rowInstant.equals(quoteInstant)) {
				return row;		// Found existing quote for this hsec and quote date
			}
			// Test for previous manual or online quote
			if ((src == ONLINE || src == MANUAL) && rowInstant.isBefore(quoteInstant)) {
	        	maxInstant = rowInstant;
	        	refQuoteRow = row;
		        if (ChronoUnit.DAYS.between(maxInstant, quoteInstant) == 1) {
		        	break;
		        }
		    }
			// Test for previous buy
	        if (src == BUY && (rowInstant.isBefore(quoteInstant) || rowInstant.equals(quoteInstant))) {
	        	maxInstant = rowInstant;
        		refQuoteRow = row;
        		if (ChronoUnit.DAYS.between(maxInstant, quoteInstant) == 0) {
		        	break;
		        }
			}
	   	}
    	
    	if (refQuoteRow != null) {
    		refQuoteRow.remove("hsp");	// Remove hsp key and value to indicate returned row is for reference only	
    	}    	
    	return refQuoteRow;
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