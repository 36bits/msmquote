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

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

public class SecTable {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(SecTable.class);

	// Instance variables
	private Table secTable;
	private IndexCursor secCursor;

	/**
	 * Constructor
	 * 
	 * @param	mnyDb
	 * @throws IOException
	 */
	public SecTable(Database mnyDb) throws IOException {
		secTable = mnyDb.getTable("SEC");
		secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
	}

	/**
	 * Update a row in the SEC table.
	 * 
	 * @param	quoteRow		
	 * @return					the hsec of the symbol or -1 if not found
	 * @throws IOException
	 */
	public int update(Map<String, Object> quoteRow) throws IOException {
		int hsec = -1;
		Map<String, Object> row = null;

		// Truncate incoming symbol if required
		String origSymbol = quoteRow.get("xSymbol").toString();
		String symbol = origSymbol;
		if (origSymbol.length() > 12) {
			symbol = origSymbol.substring(0, 12);
			LOGGER.info("Truncated symbol {} to {}", origSymbol, symbol);
		}
		
		// Find matching symbol in SEC table
		boolean found = secCursor.findFirstRow(Collections.singletonMap("szSymbol", symbol));
		if (found) {
			row = secCursor.getCurrentRow();
			hsec = (int) row.get("hsec");
			LOGGER.info("Found symbol {}: sct = {}, hsec = {}", symbol, row.get("sct"), hsec);
			// Merge quote row into SEC row and write to SEC table
			row.putAll(quoteRow);
			secCursor.updateCurrentRowFromMap(row);
			LOGGER.info("Updated quote for symbol {}", symbol);
		} else {
			LOGGER.warn("Cannot find symbol {}", symbol);
		}			
		return hsec;
	}

	/** 
	 * Get the list of investment symbols and corresponding countries.
	 * 
	 * @param
	 * @return
	 */    
	public List<String[]> getSymbols(CntryTable cntryTable) throws IOException {
		Map<String, Object> row = null;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> secIt;
		List<String[]> symbols = new ArrayList<String[]>();
		String[] symbol;

		// Build list of symbols + countries
		rowPattern.put("fOLQuotes", true);
		secIt = new IterableBuilder(secCursor).setMatchPattern(rowPattern).forward().iterator();
		while (secIt.hasNext()) {
			symbol = new String[2];
			row = secIt.next();
			if ((symbol[0] = (String) row.get("szSymbol")) != null) {
				symbol[1] = cntryTable.getCode((int) row.get("hcntry"));
				symbols.add(symbol);
			}
		}
		return symbols;
	}
}
