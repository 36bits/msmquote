package uk.co.pueblo.msmquote;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A Yahoo Finance API historical quote source.
 */
public class YahooApiHist extends YahooApiSource {

	// Constants
	//private static final Logger LOGGER = LogManager.getLogger(YahooApiHist.class);
	private static final String JSON_ROOT = "/chart/result/0";
	
	// Instance variables
	private JsonNode jn;
	private String symbol;
	private int quoteAdjuster;
	private int quoteIndex = 0;
	private String quoteType;

	/**
	 * Constructs a Yahoo Finance API historical quote source.
	 * 
	 * @param apiUrl the URL of the Yahoo Finance quote history API
	 * @throws QuoteSourceException 
	 */
	public YahooApiHist(String apiUrl) throws QuoteSourceException {
		
		// Get quote data
		jn = getJson(apiUrl);
		if (!jn.at(JSON_ROOT + "/meta").has("symbol")) {
			throw new QuoteSourceException("Received invalid quote data from Yahoo Finance historical quote data API: " + jn);
		}		

		// Get symbol and quote type
		jn =jn.at(JSON_ROOT);
		symbol = jn.at("/meta").get("symbol").asText();
		quoteType = jn.at("/meta").get("instrumentType").asText();

		// Get quote adjuster for currency
		quoteAdjuster = getAdjuster(PROPS, jn.at("/meta").get("currency").asText(), quoteType);
	}

	public Map<String, String> getNext() {
		Map<String, String> returnRow = new HashMap<>();
		if (!jn.at("/timestamp").has(quoteIndex)) {
			return returnRow;
		}

		// Process quote metadata
		returnRow.put("xSymbol", symbol);
		returnRow.put("xType", quoteType);

		// SP table columns
		int n = 1;
		String prop;
		while ((prop = PROPS.getProperty("hist.api." + n++)) != null) {
			String columnMap[] = prop.split(",");
			String value = jn.at(columnMap[0]).get(quoteIndex).asText();
			value = columnMap.length == 3 ? adjustQuote(value, columnMap[2], quoteAdjuster) : value;
			returnRow.put(columnMap[1], value);
		}

		quoteIndex++;
		return returnRow;
	}
}