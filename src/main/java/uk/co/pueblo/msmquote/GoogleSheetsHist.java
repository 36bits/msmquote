package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class GoogleSheetsHist extends GoogleSheetsSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(GoogleSheetsHist.class);
	private static final int HEADER_COLUMN = 0;
	private static final int START_COLUMN = 0;
	private static final String HEADER_FLAG = "dPrice";
	private static final String VALUE_NA = "#N/A";

	// Instance variables
	private String[] quoteMeta;

	// Constructor
	GoogleSheetsHist(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
		super(spreadsheetId, range);
		quoteMeta = range.substring(range.indexOf(" ") + 1, range.indexOf("!")).split(" ");	// symbol, quote type
	}

	public Map<String, String> getNext() throws IOException {
		Map<String, String> returnRow = new HashMap<>();
		List<Object> quoteRow;

		// Get quote row
		while (quoteIndex < quoteRows.size()) {
			quoteRow = quoteRows.get(quoteIndex++);
			if (((String) quoteRow.get(HEADER_COLUMN)).equals(HEADER_FLAG)) {
				headerRow = quoteRow;
				continue;
			}
			// Build return row
			String value;
			returnRow.put("xSymbol", quoteMeta[0]);
			returnRow.put("xType", quoteMeta[1]);
			for (int n = START_COLUMN; n < quoteRow.size(); n++) {
				if (!(value = (quoteRow.get(n)).toString()).equals(VALUE_NA)) {
					returnRow.put((String) headerRow.get(n), value);
				}
			}
			return returnRow;
		}
		return null;
	}
}