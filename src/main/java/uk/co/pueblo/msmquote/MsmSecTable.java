package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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

public class MsmSecTable {
	private static final Logger LOGGER = LogManager.getLogger(MsmSecTable.class);
	
	private Table secTable;
	private IndexCursor secCursor;
	
	// Constructor
    public MsmSecTable(Database mnyDb) throws IOException {
		secTable = mnyDb.getTable("SEC");
		secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
	}
	
    public int update(Map<String, Object> quoteRow) throws IOException {
		int hsec = -1;
	    Map<String, Object> row = null;
	    Map<String, Object> rowPattern = new HashMap<>();
		
		// Find matching symbol in SEC table
		String symbol = (String) quoteRow.get("szSymbol");
		rowPattern.put("szSymbol", symbol);
		if (secCursor.findFirstRow(rowPattern)) {
			row = secCursor.getCurrentRow();
			hsec = (int) row.get("hsec");
			LOGGER.info("Found symbol {}: sct = {}, hsec = {}", symbol, row.get("sct"), hsec);
	    	// Merge quote row into SEC row and write to SEC table
	    	row.putAll(quoteRow);
	    	row.put("dtSerial", new Date());		    	// dtSerial is assumed to be record creation/update time-stamp
	    	secCursor.updateCurrentRowFromMap(row);
	        LOGGER.info("Updated quote for symbol {}", symbol);
	   	} else {
	   		LOGGER.warn("Cannot find symbol {}", symbol);
	   	}			
		return hsec;
    }
    
    public List<String> getSymbols() throws IOException {
    	Map<String, Object> row = null;
    	Map<String, Object> rowPattern = new HashMap<>();
    	Iterator<Row> secIt;
    	List<String> symbols = new ArrayList<>();
    	String symbol;
    	    	    	
		// Build list of symbols 
    	rowPattern.put("fOLQuotes", true);
    	secIt = new IterableBuilder(secCursor).setMatchPattern(rowPattern).forward().iterator();
    	while (secIt.hasNext()) {
    		row = secIt.next();
    		symbol = (String) row.get("szSymbol");
    		if (symbol != null) {
    			symbols.add((String) row.get("szSymbol"));
    		}
    	}
    	return symbols;
    }
}
