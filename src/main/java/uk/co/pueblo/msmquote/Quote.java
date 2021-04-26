package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class Quote {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(Quote.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();

	// Instance variables
	private Map<String, int[]> summary = new HashMap<>();
	protected Properties baseProps = new Properties();
	protected boolean isQuery = false;
	
	// Define summary types
		public enum SummaryType {
			PROCESSED, WARNING
		}

	Quote(String propsRes) {
		// Set up base properties
		try {
			InputStream propsIs = YahooApiQuote.class.getClassLoader().getResourceAsStream(propsRes);
			baseProps.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
	}

	abstract Map<String, Object> getNext() throws IOException;

	/**
	 * Gets the query status of the quote.
	 *
	 * @return true if this is just a query with no update needed
	 */
	boolean isQuery() {
		return isQuery;
	}

	void incSummary(String key, SummaryType type) {
		summary.putIfAbsent(key, new int[] { 0, 0 });
		int[] count = summary.get(key);
		count[type.ordinal()]++;
		summary.put(key, count);
		return;
	}

	void logSummary(Logger logger) {
		summary.forEach((key, count) -> {
			logger.info("Summary for quote type {}: processed = {}, warnings = {}", key, count[0], count[1]);
		});
	}
}