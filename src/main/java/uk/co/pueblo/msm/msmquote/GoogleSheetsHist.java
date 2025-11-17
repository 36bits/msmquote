package uk.co.pueblo.msm.msmquote;

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
		quoteAdjuster = getAdjuster(PROPS, quoteMeta[1], null);
	}

	public Map<String, Object> getNext() throws IOException {
		Map<String, Object> returnRow = new HashMap<>();
		List<Object> quoteRow;

		// Get quote row
		while (quoteIndex < quoteRows.size()) {
			quoteRow = quoteRows.get(quoteIndex++);
			if (((String) quoteRow.get(HEADER_INDEX)).equals(HEADER_FLAG)) {
				headerRow = quoteRow;
				continue;
			}
			// Build return row
			String prop;
			int n = 0;
			returnRow.put("xSymbol", quoteMeta[0]);
			returnRow.put("xType", quoteMeta[2]);
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