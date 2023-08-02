package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A Google Sheets historical quote source.
 */
public class GoogleSheetsHist extends GoogleSheetsSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(GoogleSheetsHist.class);
	private static final int HEADER_INDEX = 0;
	private static final String HEADER_FLAG = "dPrice";
	private static final String VALUE_NA = "#N/A";

	// Instance variables
	private String[] quoteMeta;
	private int quoteAdjuster;

	/**
	 * Constructs a Google Sheets historical quote source.
	 * 
	 * @param spreadsheetId the ID of the spreadsheet containing the GOOGLEFINANCE historical quote data
	 * @param range         the range containing the quote data within the spreadsheet
	 */
	public GoogleSheetsHist(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
		super(spreadsheetId, range);
		quoteMeta = range.substring(range.indexOf(" ") + 1, range.indexOf("!")).split(" "); // symbol, currency, quote type
		quoteAdjuster = getAdjuster(PROPS, quoteMeta[1]);
	}

	public Map<String, String> getNext() throws IOException {
		Map<String, String> returnRow = new HashMap<>();
		List<Object> quoteRow;

		// Get quote row
		while (quoteIndex < quoteRows.size()) {
			quoteRow = quoteRows.get(quoteIndex++);
			if (((String) quoteRow.get(HEADER_INDEX)).equals(HEADER_FLAG)) {
				headerRow = quoteRow;
				continue;
			}
			// Build return row
			String headerCol;
			String value;
			int n = 0;
			returnRow.put("xSymbol", quoteMeta[0]);
			returnRow.put("xType", quoteMeta[2]);
			for (Object object : quoteRow) {
				value = object.toString();
				if (!value.equals(VALUE_NA)) {
					headerCol = (String) headerRow.get(n);
					returnRow.put((String) headerCol, adjustQuote(value, PROPS.getProperty("column." + headerCol), quoteAdjuster));
				}
				n++;
			}
			break;
		}
		return returnRow;
	}
}