package uk.co.pueblo.msmquote.msm;

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

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.util.IterableBuilder;

public class SpTable extends MsmTable {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(SpTable.class);
	private static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	private static final int SRC_BUY = 1;
	private static final int SRC_MANUAL = 5;
	private static final int SRC_ONLINE = 6;
	
	// Class variables
	private static int hsp = 0;

	// Instance variables
	private ArrayList<Map<String, Object>> spRowList;

	/**
	 * Constructor for the securities price table.
	 * 
	 * @param msmDb the opened MS Money file
	 * @throws IOException
	 */
	public SpTable(Database msmDb) throws IOException {
		super(msmDb, "SP");

		// Get current hsp (SP table index)
		msmCursor.afterLast();
		if (msmCursor.getPreviousRow() != null) {
			hsp = (int) msmCursor.getCurrentRowValue(msmTable.getColumn("hsp"));
		}
		LOGGER.debug("Current highest hsp = {}", hsp);

		spRowList = new ArrayList<>();
	}

	/** 
	 * Updates the SP table with the supplied quote row.
	 *
	 * @param	quoteRow	SP table row containing the quote data to be updated
	 * @param	hsec		the hsec to be updated
	 * @throws	IOException
	 */
	public void update(Map<String, Object> quoteRow, int hsec) throws IOException {

		// Truncate incoming symbol if required, for cosmetic reasons only
		String symbol = quoteRow.get("xSymbol").toString();
		if (symbol.length() > 12) {
			symbol = symbol.substring(0, 12);
		}

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
			hsp++;
			row.put("hsp", hsp);
			row.put("src", SRC_ONLINE);
			spRowList.add(row);
			LOGGER.info("Added new quote for symbol {} to table update list: {}, new price = {}, new hsp = {}", symbol, row.get("dt"), row.get("dPrice"), row.get("hsp"));
		} else {
			msmCursor.updateCurrentRowFromMap(row);
			LOGGER.info("Updated previous quote for symbol {}: {}, new price = {}", symbol, row.get("dt"), row.get("dPrice"));
		}		

		return;
	}

	public void addNewRows() throws IOException{
		if (!spRowList.isEmpty()) {
			msmTable.addRowsFromMaps(spRowList);
			LOGGER.info("Added new quotes from table update list");
		}
		return;
	}

	/** 
	 * Searches the SP table for a row matching the supplied hsec and date.
	 *
	 * @param	hsec	the hsec to find
	 * @param	date	the date to find
	 * @return	 		the SP row if match found, null if no row for hsec found or
	 * 					row containing 'xRefOnly' key if hsec found but not date
	 * @throws	IOException
	 */
	private Map<String, Object> getSpRow(int hsec, LocalDateTime quoteDate) throws IOException {
		Map<String, Object> row;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> spIt;
		Instant firstInstant;
		Instant lastInstant;

		// Get instants for date from first and last rows for hsec
		rowPattern.put("hsec", hsec);
		spIt = new IterableBuilder(msmCursor).setMatchPattern(rowPattern).forward().iterator();
		if (!spIt.hasNext()) {
			return null;	// No rows in SP table for this hsec
		} else {
			row = spIt.next();
			firstInstant = ZonedDateTime.of((LocalDateTime) row.get("dt"), SYS_ZONE_ID).toInstant();
			spIt = new IterableBuilder(msmCursor).setMatchPattern(rowPattern).reverse().iterator();
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
			spIt = new IterableBuilder(msmCursor).setMatchPattern(rowPattern).reverse().iterator();
		} else {
			spIt = new IterableBuilder(msmCursor).setMatchPattern(rowPattern).forward().iterator();
		}

		Map<String, Object> returnRow = null;
		Instant rowInstant = Instant.ofEpochMilli(0);
		Instant maxInstant = Instant.ofEpochMilli(0);

		while (spIt.hasNext()) {
			row = spIt.next();
			LOGGER.debug(row);
			int src = (int) row.get("src");
			rowInstant = ZonedDateTime.of((LocalDateTime) row.get("dt"), SYS_ZONE_ID).toInstant();
			if ((src == SRC_ONLINE || src == SRC_MANUAL) && rowInstant.equals(quoteInstant)) {
				return row;		// Found existing quote for this hsec and quote date
			}
			if (rowInstant.isBefore(maxInstant)) {
				continue;
			}
			// Test for previous manual or online quote
			if ((src == SRC_ONLINE || src == SRC_MANUAL) && rowInstant.isBefore(quoteInstant)) {
				maxInstant = rowInstant;
				returnRow = row;
				continue;
			}
			// Test for previous buy
			if (src == SRC_BUY && (rowInstant.isBefore(quoteInstant) || rowInstant.equals(quoteInstant))) {
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