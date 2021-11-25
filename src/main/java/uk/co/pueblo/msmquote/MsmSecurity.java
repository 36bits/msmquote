package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.util.IterableBuilder;

class MsmSecurity {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmSecurity.class);
	private static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	private static final String PROPS_FILE = "MsmSecurity.properties";
	private static final Properties PROPS = new Properties();
	private static final String SEC_TABLE = "SEC";
	private static final String SP_TABLE = "SP";
	private static final int SRC_BUY = 1;
	private static final int SRC_MANUAL = 5;
	private static final int SRC_ONLINE = 6;
	private static final int UPDATE_OK = 0;
	private static final int UPDATE_WARN = 1;
	private static final int UPDATE_ERROR = 2;

	// Instance variables
	private final Table secTable;
	private final Table spTable;
	private ArrayList<Map<String, Object>> spRowAddList = new ArrayList<>();
	private int hsp = 0;
	private Map<String, int[]> summary = new HashMap<>();

	// Constructor
	MsmSecurity(Database msmDb) throws IOException {

		// Open properties
		try {
			InputStream propsIs = QuoteSource.class.getClassLoader().getResourceAsStream(PROPS_FILE);
			PROPS.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}

		// Open the securities tables
		secTable = msmDb.getTable(SEC_TABLE);
		spTable = msmDb.getTable(SP_TABLE);

		// Get current hsp (SP table index)
		IndexCursor spCursor = CursorBuilder.createCursor(spTable.getPrimaryKeyIndex());
		spCursor.afterLast();
		if (spCursor.getPreviousRow() != null) {
			hsp = (int) spCursor.getCurrentRowValue(spTable.getColumn("hsp"));
		}
		LOGGER.debug("Current highest hsp = {}", hsp);
	}

	/**
	 * Update the SEC and SP tables with the supplied quote row.
	 * 
	 * @param quoteRow the row containing the quote data to update
	 * @return 0 update OK; 1 update with warnings; 2 update with errors
	 * @throws IOException
	 */
	int update(Map<String, Object> quoteRow) throws IOException {

		// Validate incoming row and process status
		quoteRow = validate(quoteRow);
		int updateStatus = (int) quoteRow.get("xStatus");
		String quoteType = quoteRow.get("xType").toString();
		if (updateStatus == UPDATE_ERROR) {
			incSummary(quoteType, updateStatus);
			return updateStatus;
		}

		String symbol = quoteRow.get("xSymbol").toString();
		LOGGER.info("Processing quote data for symbol {}, quote type = {}", symbol, quoteType);

		// Truncate symbol if required
		String origSymbol = symbol;
		if (origSymbol.length() > 12) {
			symbol = origSymbol.substring(0, 12);
			LOGGER.info("Truncated symbol {} to {}", origSymbol, symbol);
			quoteRow.put("xSymbol", symbol);
		}

		// Add dtSerial to quote row
		// TODO Confirm assumption that dtSerial is time-stamp of quote update
		quoteRow.put("dtSerial", LocalDateTime.now());

		// Update SEC table with quote row
		int hsec = -1;
		Map<String, Object> secRow = null;
		IndexCursor secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
		boolean found = secCursor.findFirstRow(Collections.singletonMap("szSymbol", symbol));
		if (found) {
			secRow = secCursor.getCurrentRow();
			hsec = (int) secRow.get("hsec");
			LOGGER.info("Found symbol {} in SEC table: sct = {}, hsec = {}", symbol, secRow.get("sct"), hsec);
			// Merge quote row into SEC row and write to SEC table
			secRow.putAll(quoteRow); // TODO Should secRow be sanitised first?
			secCursor.updateCurrentRowFromMap(secRow);
			LOGGER.info("Updated SEC table for symbol {}", symbol);
		} else {
			LOGGER.warn("Cannot find symbol {} in SEC table", symbol);
			incSummary(quoteType, UPDATE_WARN);
			return UPDATE_WARN;
		}

		// Update summary
		incSummary(quoteType, updateStatus);

		// Update SP table with quote row
		LocalDateTime quoteDate = (LocalDateTime) quoteRow.get("dt");
		Map<String, Object> spRowPattern = new HashMap<>();
		Map<String, Object> spRow = new HashMap<>();
		Map<String, Object> prevSpRow = new HashMap<>();
		spRowPattern.put("hsec", hsec);
		IndexCursor spCursor = CursorBuilder.createCursor(spTable.getPrimaryKeyIndex());
		Iterator<Row> spIt = new IterableBuilder(spCursor).setMatchPattern(spRowPattern).forward().iterator();
		if (spIt.hasNext()) {
			// Get instants for date from first and last rows for hsec
			spRow = spIt.next();
			Instant firstInstant = ZonedDateTime.of((LocalDateTime) spRow.get("dt"), SYS_ZONE_ID).toInstant();
			spIt = new IterableBuilder(spCursor).setMatchPattern(spRowPattern).reverse().iterator();
			spRow = spIt.next();
			Instant lastInstant = ZonedDateTime.of((LocalDateTime) spRow.get("dt"), SYS_ZONE_ID).toInstant();

			// Build iterator with the closest date to the quote date
			Instant quoteInstant = ZonedDateTime.of(quoteDate, SYS_ZONE_ID).toInstant();
			long firstDays = Math.abs(ChronoUnit.DAYS.between(firstInstant, quoteInstant));
			long lastDays = Math.abs(ChronoUnit.DAYS.between(lastInstant, quoteInstant));
			LOGGER.debug("Instants: first = {}, last = {}, quote = {}", firstInstant, lastInstant, quoteInstant);
			LOGGER.debug("Days: first->quote = {}, last->quote = {}", firstDays, lastDays);

			if (lastDays < firstDays) {
				spIt = new IterableBuilder(spCursor).setMatchPattern(spRowPattern).reverse().iterator();
			} else {
				spIt = new IterableBuilder(spCursor).setMatchPattern(spRowPattern).forward().iterator();
			}

			// Search SP table for existing quote or most recent previous quote
			Instant rowInstant = Instant.ofEpochMilli(0);
			Instant maxInstant = Instant.ofEpochMilli(0);
			while (spIt.hasNext()) {
				spRow = spIt.next();
				LOGGER.debug(spRow);
				int src = (int) spRow.get("src");
				rowInstant = ZonedDateTime.of((LocalDateTime) spRow.get("dt"), SYS_ZONE_ID).toInstant();
				if ((src == SRC_ONLINE || src == SRC_MANUAL) && rowInstant.equals(quoteInstant)) {
					// Found existing quote for this hsec and quote date so update SP table
					spRow.putAll(quoteRow); // TODO Should spRow be sanitised first?
					spCursor.updateCurrentRowFromMap(spRow);
					LOGGER.info("Updated previous quote for symbol {} in SP table: {}, new price = {}", symbol, spRow.get("dt"), spRow.get("dPrice"));
					return updateStatus;
				}
				if (rowInstant.isBefore(maxInstant)) {
					continue;
				}
				// Test for previous manual or online quote
				if ((src == SRC_ONLINE || src == SRC_MANUAL) && rowInstant.isBefore(quoteInstant)) {
					maxInstant = rowInstant;
					prevSpRow = spRow;
					continue;
				}
				// Test for previous buy
				if (src == SRC_BUY && (rowInstant.isBefore(quoteInstant) || rowInstant.equals(quoteInstant))) {
					maxInstant = rowInstant;
					prevSpRow = spRow;
				}
			}
		}

		if (prevSpRow.isEmpty()) {
			LOGGER.info("Cannot find previous quote for symbol {} in SP table", symbol);
		} else {
			LOGGER.info("Found previous quote for symbol {} in SP table: {}, price = {}, hsp = {}", symbol, prevSpRow.get("dt"), prevSpRow.get("dPrice"), prevSpRow.get("hsp"));
		}

		// Add to SP row add list
		hsp++;
		spRow.put("hsp", hsp);
		spRow.put("hsec", hsec);
		spRow.put("src", SRC_ONLINE);
		spRow.putAll(quoteRow); // TODO Should spRow be sanitised first?
		spRowAddList.add(spRow);
		LOGGER.info("Added new quote for symbol {} to SP table update list: {}, new price = {}, new hsp = {}", symbol, spRow.get("dt"), spRow.get("dPrice"), spRow.get("hsp"));

		return updateStatus;
	}

	void addNewSpRows() throws IOException {
		if (!spRowAddList.isEmpty()) {
			spTable.addRowsFromMaps(spRowAddList);
			LOGGER.info("Added new quotes to SP table from table update list");
		}
		return;
	}

	/**
	 * Create a list of investment symbols and corresponding country codes.
	 * 
	 * @param cntryTable the country table
	 * @return the list of symbols and corresponding countries
	 * @throws IOException
	 */
	List<String[]> getSymbols(MsmCommon msmCore) throws IOException {
		Map<String, Object> row = null;
		Map<String, Object> rowPattern = new HashMap<>();
		Iterator<Row> secIt;
		List<String[]> symbols = new ArrayList<String[]>();
		String[] symbol;

		// Build list of symbols + countries
		rowPattern.put("fOLQuotes", true);
		IndexCursor secCursor = CursorBuilder.createCursor(secTable.getPrimaryKeyIndex());
		secIt = new IterableBuilder(secCursor).setMatchPattern(rowPattern).forward().iterator();
		while (secIt.hasNext()) {
			symbol = new String[2];
			row = secIt.next();
			if ((symbol[0] = (String) row.get("szSymbol")) != null) {
				symbol[1] = msmCore.getCntryCode((int) row.get("hcntry"));
				symbols.add(symbol);
			}
		}
		return symbols;
	}

	private static Map<String, Object> validate(Map<String, Object> quoteRow) {

		String prop;
		String props[];
		int column = 1;

		// Validate required columns
		String reqLogMsg = "";
		while ((prop = PROPS.getProperty("column." + column++)) != null) {
			props = prop.split(",");
			if (!quoteRow.containsKey(props[0])) {
				if (props.length == 2) {
					quoteRow.put(props[0], props[1]);		// apply default value
				}
				// Add column to log message
				if (reqLogMsg.isEmpty()) {
					reqLogMsg = props[0];
				} else {
					reqLogMsg = reqLogMsg + ", " + props[0];
				}
			}
		}

		// Emit log message and return if necessary
		if (!reqLogMsg.isEmpty()) {
			LOGGER.error("Required quote data missing for symbol {}: {}", quoteRow.get("xSymbol"), reqLogMsg);
			quoteRow.put("xStatus", UPDATE_ERROR);
			return quoteRow;
		}

		// Validate optional columns
		String optLogMsg[][] = { { "Optional quote data missing", "", "" }, { "Default values applied", "", "" } };
		String quoteType = quoteRow.get("xType").toString();
		column = 1;
		int updateStatus = UPDATE_OK;
		int i;
		while ((prop = PROPS.getProperty("column." + quoteType + "." + column++)) != null) {
			props = prop.split(",");
			if (!quoteRow.containsKey(props[0])) {
				updateStatus = UPDATE_WARN;
				for (i = 0; i < optLogMsg.length; i++) {
					if (i == 0) {
						optLogMsg[i][1] = props[0];
					}
					if (i == 1 && props.length == 2) {
						optLogMsg[i][1] = props[0];		
						quoteRow.put(props[0], props[1]);	// apply default value
					}
					// Append to respective log message
					if (optLogMsg[i][2].isEmpty()) {
						optLogMsg[i][2] = optLogMsg[i][1];
					} else {
						optLogMsg[i][2] = optLogMsg[i][2] + ", " + optLogMsg[i][1];
					}
					optLogMsg[i][1] = "";
				}
			}
		}

		// Emit log messages
		for (i = 0; i < optLogMsg.length; i++) {
			if (!optLogMsg[i][2].isEmpty()) {
				LOGGER.warn("{} for symbol {}: {}", optLogMsg[i][0], quoteRow.get("xSymbol").toString(), optLogMsg[i][2]);
			}
		}

		quoteRow.put("xStatus", updateStatus);
		return quoteRow;
	}

	private void incSummary(String key, int index) {
		summary.putIfAbsent(key, new int[] { 0, 0, 0 }); // OK, warnings, errors
		int[] count = summary.get(key);
		count[index]++;
		summary.put(key, count);
		return;
	}

	protected void logSummary() {
		summary.forEach((key, count) -> {
			LOGGER.info("Summary for quote type {}: OK = {}, warnings = {}, errors = {}", key, count[0], count[1], count[2]);
		});
	}
}