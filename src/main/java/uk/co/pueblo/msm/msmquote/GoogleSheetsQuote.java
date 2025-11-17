package uk.co.pueblo.msm.msmquote;

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

	/**
	 * Constructs a Google Sheets quote source.
	 * 
	 * @param spreadsheetId the ID of the spreadsheet containing the GOOGLEFINANCE
	 *                      quote data
	 * @param range         the range containing the quote data within the
	 *                      spreadsheet
	 */
	public GoogleSheetsQuote(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
		super(spreadsheetId, range);
	}

	public Map<String, Object> getNext() throws IOException {
		Map<String, Object> returnRow = new HashMap<>();
		List<Object> quoteRow;

		// Get quote row
		while (quoteIndex < quoteRows.size()) {
			quoteRow = quoteRows.get(quoteIndex++);
			if (quoteRow.get(HEADER_INDEX).toString().equals(HEADER_FLAG)) {
				headerRow = quoteRow;
				continue;
			}
			// Build return row
			String prop;
			int quoteAdjuster = 1;
			int n = 0;
			for (Object object : quoteRow) {
				String objectStr = object.toString(); // Google Sheets cell values are all presented as string objects
				String headerCol = headerRow.get(n).toString();
				if (headerCol.equals("currency")) {
					quoteAdjuster = getAdjuster(PROPS, objectStr, null);
				} else if (!MATCH_NUM.reset(objectStr).matches()) {
					returnRow.put(headerCol, objectStr);
				} else if ((prop = PROPS.getProperty("column." + headerCol)) != null) {
					returnRow.put(headerCol, adjustQuote((Double.parseDouble(objectStr)), prop, quoteAdjuster));
				} else {
					returnRow.put(headerCol, (Double.parseDouble(objectStr)));
				}
				n++;
			}
			break;
		}
		return returnRow;
	}
}