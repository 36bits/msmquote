package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class MsmInstrument {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(MsmInstrument.class);
	static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
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
				InputStream propsIs = getClass().getClassLoader().getResourceAsStream(propsFile);
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
		int pass;
		String logMsg[] = { "Required quote data missing", "Required default values applied", "Optional quote data missing", "Optional default values applied" };
		Level logLevel[] = { Level.ERROR, Level.ERROR, Level.WARN, Level.WARN };
		String missingCols[] = { "", "", "", "" }; // required, required defaults, optional, optional defaults
		String columnSet = "column.";

		// Validate
		for (pass = 0; pass < missingCols.length; pass += 2) {
			if (pass == 2) {
				columnSet = columnSet + quoteRow.get("xType").toString() + ".";
				column = 1;
			}
			while ((prop = props.getProperty(columnSet + column++)) != null) {
				propArray = prop.split(",");
				if (!quoteRow.containsKey(propArray[0])) {
					missingCols[pass] = missingCols[pass] + propArray[0] + ", ";
					// Process default value
					if (propArray.length == 2) {
						if (propArray[0].startsWith("dt")) {
							// Process LocalDateTime values
							quoteRow.put(propArray[0], Instant.ofEpochSecond(Long.parseLong(propArray[1])).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay());
						} else if (propArray[0].startsWith("d") || propArray[1].matches("\\d+\\.\\d+")) {
							// Process Double values
							quoteRow.put(propArray[0], Double.parseDouble(propArray[1]));
						} else {
							// And finally assume everything else is a Long value
							quoteRow.put(propArray[0], Long.parseLong(propArray[1]));
						}
						missingCols[pass + 1] = missingCols[pass + 1] + propArray[0] + ", ";
					}
				}
			}
		}

		// Emit log messages
		int status = UPDATE_OK;
		int maxStatus = UPDATE_OK;
		for (pass = 0; pass < missingCols.length; pass++) {
			if (!missingCols[pass].isEmpty()) {
				LOGGER.log(logLevel[pass], "{} for symbol {}: {}", logMsg[pass], quoteRow.get("xSymbol").toString(), missingCols[pass].substring(0, missingCols[pass].length() - 2));
				if ((status = 4 - logLevel[pass].intLevel() / 100) > maxStatus) {
					maxStatus = status;
				}
			}
		}

		quoteRow.put("xStatus", maxStatus);
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