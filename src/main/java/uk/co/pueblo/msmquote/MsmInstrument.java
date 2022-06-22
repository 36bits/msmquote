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

	abstract int update(Map<String, String> inRow) throws IOException;

	Map<String, Object> buildMsmRow(Map<String, String> inRow) {
		Map<String, Object> returnRow = new HashMap<>();
		String prop;
		int column = 1;
		int pass;
		String logMsg[] = { "Required quote data missing", "Required default values applied", "Optional quote data missing", "Optional default values applied" };
		Level logLevel[] = { Level.ERROR, Level.ERROR, Level.WARN, Level.WARN };
		String missingCols[] = { "", "", "", "" }; // required, required defaults, optional, optional defaults
		String columnSet = "column.";

		// Validate
		for (pass = 0; pass < missingCols.length; pass += 2) {
			if (pass == 2) {
				columnSet = columnSet + inRow.get("xType") + ".";
				column = 1;
			}
			while ((prop = props.getProperty(columnSet + column++)) != null) {
				String propArray[] = prop.split(",");
				String value;

				if (inRow.containsKey(propArray[0])) {
					value = inRow.get(propArray[0]);
				} else {
					// Get default value
					missingCols[pass] = missingCols[pass] + propArray[0] + ", ";
					if (propArray.length == 2) {
						value = propArray[1];
						missingCols[pass + 1] = missingCols[pass + 1] + propArray[0] + ", ";
					} else {
						continue;
					}
				}
				
				// Now build MSM row
				if (propArray[0].startsWith("dt") && value.matches("\\d+")) {
					// Process LocalDateTime value in epoch seconds
					returnRow.put(propArray[0], Instant.ofEpochSecond(Long.parseLong(value)).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay());
				} else if (propArray[0].startsWith("dt")) {
					// Process LocalDateTime value in UTC format
					value = value.matches("^\\d{4}\\-\\d{2}\\-\\d{2}$") ? value + "T00:00:00Z" : value;
					returnRow.put(propArray[0], Instant.parse(value).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay());
				} else if (propArray[0].startsWith("d") || value.matches("\\d+\\.\\d+")) {
					// Process Double values
					returnRow.put(propArray[0], Double.parseDouble(value));
				} else if (propArray[0].startsWith("x")) {
					// Process msmquote internal values
					returnRow.put(propArray[0], value);
				} else {
					// And finally assume everything else is a Long value
					returnRow.put(propArray[0], Long.parseLong(value));
				}
			}
		}

		// Emit log messages
		int status = UPDATE_OK;
		int maxStatus = UPDATE_OK;
		for (pass = 0; pass < missingCols.length; pass++) {
			if (!missingCols[pass].isEmpty()) {
				LOGGER.log(logLevel[pass], "{} for symbol {}: {}", logMsg[pass], returnRow.get("xSymbol").toString(), missingCols[pass].substring(0, missingCols[pass].length() - 2));
				if ((status = 4 - logLevel[pass].intLevel() / 100) > maxStatus) {
					maxStatus = status;
				}
			}
		}

		returnRow.put("xStatus", maxStatus);
		return returnRow;
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