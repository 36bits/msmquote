package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A Google Sheets quote source.
 */
public class GoogleSheetsQuote extends GoogleSheetsSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(GoogleSheetsQuote.class);
	private static final int HEADER_INDEX = 0;
	private static final String HEADER_FLAG = "xSymbol";
	private static final String VALUE_NA = "#N/A";

	/**
	 * Constructs a Google Sheets quote source.
	 * 
	 * @param spreadsheetId	the ID of the spreadsheet containing the GOOGLEFINANCE quote data
	 * @param range			the range containing the quote data within the spreadsheet
	 */
	public GoogleSheetsQuote(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
		super(spreadsheetId, range);
	}

	public Map<String, String> getNext() throws IOException {
		Map<String, String> returnRow = new HashMap<>();
		List<Object> quoteRow;

		// Get quote row
		while (quoteIndex < quoteRows.size()) {
			quoteRow = quoteRows.get(quoteIndex++);
			if (quoteRow.get(HEADER_INDEX).toString().equals(HEADER_FLAG)) {
				headerRow = quoteRow;
				continue;
			}
			// Build return row
			String headerCol;
			String value;
			int quoteAdjuster = 1;
			int n = 0;
			for (Object object : quoteRow) {
				value = object.toString();
				if (!value.equals(VALUE_NA)) {
					headerCol = (String) headerRow.get(n);
					if (headerCol.equals("currency")) {
						quoteAdjuster = getAdjuster(PROPS, value);
					} else {
						returnRow.put((String) headerCol, adjustQuote(value, PROPS.getProperty("column." + headerCol), quoteAdjuster));
					}
				}
				n++;
			}
			break;
		}
		return returnRow;
	}
}