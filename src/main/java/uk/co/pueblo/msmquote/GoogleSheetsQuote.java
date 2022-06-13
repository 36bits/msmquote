package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class GoogleSheetsQuote extends GoogleSheetsSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(GoogleSheetsQuote.class);
	private static final int HEADER_COLUMN = 0;
	private static final String HEADER_FLAG = "xSymbol";
	private static final String VALUE_NA = "#N/A";

	// Constructor
	GoogleSheetsQuote(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
		super(spreadsheetId, range);
	}

	public Map<String, String> getNext() throws IOException {
		Map<String, String> returnRow = new HashMap<>();
		List<Object> quoteRow;

		// Get quote row
		while (quoteIndex < quoteRows.size()) {
			quoteRow = quoteRows.get(quoteIndex++);
			if (quoteRow.get(HEADER_COLUMN).toString().equals(HEADER_FLAG)) {
				headerRow = quoteRow;
				continue;
			}
			// Build return row
			String value;
			for (int n = 0; n < quoteRow.size(); n++) {
				if (!(value = (quoteRow.get(n)).toString()).equals(VALUE_NA)) {
					returnRow.put((String) headerRow.get(n), value);
				}
			}
			return returnRow;
		}
		return null;
	}
}