package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class MsmInstrument {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(MsmInstrument.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	// static final Properties PROPS = new Properties();
	static final int UPDATE_OK = 0;
	static final int UPDATE_WARN = 1;
	static final int UPDATE_ERROR = 2;

	// Instance variables
	Map<String, int[]> summary = new HashMap<>();
	Properties props = new Properties();

	// Constructor
	MsmInstrument(String propsFile) {
		// Open properties
		if (!propsFile.isEmpty()) {
			try {
				InputStream propsIs = QuoteSource.class.getClassLoader().getResourceAsStream(propsFile);
				props.load(propsIs);
			} catch (IOException e) {
				LOGGER.fatal(e);
			}
		}
	}

	abstract int update(Map<String, Object> quoteRow) throws IOException;

	Map<String, Object> validate(Map<String, Object> quoteRow) {

		String prop;
		String propArray[];
		int column = 1;

		// Validate required columns
		String reqLogMsg = "";
		while ((prop = props.getProperty("column." + column++)) != null) {
			propArray = prop.split(",");
			if (!quoteRow.containsKey(propArray[0])) {
				if (propArray.length == 2) {
					quoteRow.put(propArray[0], propArray[1]);		// apply default value
				}
				// Add column to log message
				if (reqLogMsg.isEmpty()) {
					reqLogMsg = propArray[0];
				} else {
					reqLogMsg = reqLogMsg + ", " + propArray[0];
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
		while ((prop = props.getProperty("column." + quoteType + "." + column++)) != null) {
			propArray = prop.split(",");
			if (!quoteRow.containsKey(propArray[0])) {
				updateStatus = UPDATE_WARN;
				for (i = 0; i < optLogMsg.length; i++) {
					if (i == 0) {
						optLogMsg[i][1] = propArray[0];
					}
					if (i == 1 && propArray.length == 2) {
						optLogMsg[i][1] = propArray[0];
						quoteRow.put(propArray[0], propArray[1]);	// apply default value
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

	void incSummary(String key, int index) {
		summary.putIfAbsent(key, new int[] { 0, 0, 0 }); // OK, warnings, errors
		int[] count = summary.get(key);
		count[index]++;
		summary.put(key, count);
		return;
	}

	void logSummary() {
		summary.forEach((key, count) -> {
			LOGGER.info("Summary for quote type {}: OK = {}, warnings = {}, errors = {}", key, count[0], count[1], count[2]);
		});
	}
}