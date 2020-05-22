package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

public class MsmSpTable {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmSpTable.class);
	private static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();

	// Instance variables
	private Table spTable;
	private IndexCursor spCursor;
	private ArrayList<Map<String, Object>> spRowList;
	private int hsp;

	// Define SP table src values
	private enum Src {
		BUY(1), MANUAL(5), ONLINE(6);

		private final int code;

		Src(int code) {
			this.code = code;				
		}

		public int getCode() {
			return code;
		}
	}

	/**
	 * Constructor
	 * 
	 * @param mnyDb
	 * @throws IOException
	 */
	public MsmSpTable(Database mnyDb) throws IOException {
		spTable = mnyDb.getTable("SP");
		spCursor = CursorBuilder.createCursor(spTable.getPrimaryKeyIndex());

		// Get current hsp (SP table index)
		int hsp = 0;
		spCursor.afterLast();
		if (spCursor.getPreviousRow() != null) {
			hsp = (int) spCursor.getCurrentRowValue(spTable.getColumn("hsp"));
		}
		LOGGER.debug("Current highest hsp = {}", hsp);

		spRowList = new ArrayList<>();
	}

	/** 
	 * Updates the SP table with the supplied quote row
	 *
	 * @param	quoteRow	SP table row containing the quote data to be updated
	 * @param	hsec		the hsec to be updated
	 */
	public void update(Map<String, Object> quoteRow, int hsec) throws IOException {
		String symbol = (String) quoteRow.get("szSymbol");
		Map<String, Object> row = null;

		// Build SP row
		LocalDateTime quoteDate = (LocalDateTime) quoteRow.get("dt");
		boolean addRow = false;
		if ((row = getSpRow(hsec, quoteDate)) == null) {
			LOGGER.info("Cannot find previous quote for symbol {}", symbol);
			row = quoteRow;
			row.put("hsec", hsec);
			addRow = true;
		} else {  
			if (row.containsKey("xRefOnly")) {
				LOGGER.info("Found previous quote for symbol {}: {}, price = {}, hsp = {}", symbol, row.get("dt"), row.get("dPrice"), row.get("hsp"));
				addRow = true;
			} else {
				LOGGER.info("Found previous quote to update for symbol {}: {}, price = {}, hsp = {}", symbol, row.get("dt"), row.get("dPrice"), row.get("hsp"));
			}
			// Merge quote row into SP row
			row.putAll(quoteRow);
		}

		// Update SP row
		if (addRow) {
			hsp = hsp + 1;
			row.put("hsp", hsp);
			row.put("src", Src.ONLINE.getCode());
			spRowList.add(row);
			LOGGER.info("Added new quote for symbol {} to table update list: {}, new price = {}, new hsp = {}", symbol, row.get("dt"), row.get("dPrice"), row.get("hsp"));
		} else {
			spCursor.updateCurrentRowFromMap(row);
			LOGGER.info("Updated previous quote for symbol {}: {}, new price = {}", symbol, row.get("dt"), row.get("dPrice"));
		}		

		return;
	}

	public void addNewRows() throws IOException{
		if (!spRowList.isEmpty()) {
			spTable.addRowsFromMaps(spRowList);
			LOGGER.info("Added new quotes from table update list");
		}
		return;
	}

	/** 
	 * Searches the SP table for a row matching the supplied hsec and date.
	 *
	 * @param	hsec	hsec for search
	 * @param	date	date for search
	 * @return	 		the SP row if match found, null if no row for hsec found or
	 * 					row containing 'xRefOnly' key if hsec found but not date 
	 */
	private Map<String, Object> getSpRow(int hsec, LocalDateTime quoteDate) throws IOException {
		Map<String, Object> row;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> spIt;
		Instant firstInstant;
		Instant lastInstant;

		// Get instants for date from first and last rows for hsec
		rowPattern.put("hsec", hsec);
		spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).forward().iterator();
		if (!spIt.hasNext()) {
			return null;	// No rows in SP table for this hsec
		} else {
			row = spIt.next();
			firstInstant = ZonedDateTime.of((LocalDateTime) row.get("dt"), SYS_ZONE_ID).toInstant();
			spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).reverse().iterator();
			row = spIt.next();
			lastInstant = ZonedDateTime.of((LocalDateTime) row.get("dt"), SYS_ZONE_ID).toInstant();
		}		

		// Build iterator with the closest date to the quote date
		Instant quoteInstant = ZonedDateTime.of(quoteDate, SYS_ZONE_ID).toInstant();
		long firstDays = Math.abs(ChronoUnit.DAYS.between(firstInstant, quoteInstant));
		long lastDays = Math.abs(ChronoUnit.DAYS.between(lastInstant, quoteInstant));
		LOGGER.debug("Instants: first = {}, last = {}, quote = {}", firstInstant, lastInstant, quoteInstant);
		LOGGER.debug("Days: first->quote = {}, last->quote = {}", firstDays, lastDays);

		if (lastDays < firstDays) {
			spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).reverse().iterator();
		} else {
			spIt = new IterableBuilder(spCursor).setMatchPattern(rowPattern).forward().iterator();
		}

		Map<String, Object> returnRow = null;
		Instant rowInstant = Instant.ofEpochMilli(0);
		Instant maxInstant = Instant.ofEpochMilli(0);

		while (spIt.hasNext()) {
			row = spIt.next();
			LOGGER.debug(row);
			int src = (int) row.get("src");
			rowInstant = ZonedDateTime.of((LocalDateTime) row.get("dt"), SYS_ZONE_ID).toInstant();
			if ((src == Src.ONLINE.getCode() || src == Src.MANUAL.getCode()) && rowInstant.equals(quoteInstant)) {
				return row;		// Found existing quote for this hsec and quote date
			}
			if (rowInstant.isBefore(maxInstant)) {
				continue;
			}
			// Test for previous manual or online quote
			if ((src == Src.ONLINE.getCode() || src == Src.MANUAL.getCode()) && rowInstant.isBefore(quoteInstant)) {
				maxInstant = rowInstant;
				returnRow = row;
				continue;
			}
			// Test for previous buy
			if (src == Src.BUY.getCode() && (rowInstant.isBefore(quoteInstant) || rowInstant.equals(quoteInstant))) {
				maxInstant = rowInstant;
				returnRow = row;
			}
		}

		if (returnRow != null) {
			returnRow.put("xRefOnly", null);		// 'xRefOnly' key indicates returned row is for reference only
		}    	
		return returnRow;
	}	
}