package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

class YahooApiHist extends YahooSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooApiHist.class);
	private static final String PROPS_FILE = "YahooSource.properties";

	// Instance variables
	private JsonNode resultJn;
	private String symbol;
	private int quoteDivisor;
	private int quoteMultiplier;
	private int quoteIndex;
	private String quoteType;

	/**
	 * Constructor.
	 * 
	 * @param apiUrl the URL of the Yahoo Finance quote history API
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException 
	 * @throws APIException 
	 */
	YahooApiHist(String apiUrl) throws IOException, InterruptedException, URISyntaxException, APIException {
		super(PROPS_FILE);

		// Get symbol and quote type
		resultJn = getJson(apiUrl).at("/chart/result/0");
		symbol = resultJn.at("/meta").get("symbol").asText();
		quoteType = resultJn.at("/meta").get("instrumentType").asText();
		
		// Get divisor or multiplier for quote currency and quote type
		String currency = resultJn.at("/meta").get("currency").asText();
		quoteDivisor = getDivisor(currency, quoteType);
		quoteMultiplier = getMultiplier(currency, quoteType);
		quoteIndex = 0;
	}

	/**
	 * Gets the next row of quote data from the JSON node.
	 * 
	 * @return the quote row or null if no more data
	 */
	public Map<String, String> getNext() {
		Map<String, String> returnRow = new HashMap<>();
		if (!resultJn.at("/timestamp").has(quoteIndex)) {
			return null;
		}

		// Process quote metadata
		returnRow.put("xSymbol", symbol);
		returnRow.put("xType", quoteType);

		// SP table columns
		try {
			int n = 1;
			String prop;
			while ((prop = PROPS.getProperty("hist.api." + n++)) != null) {
				String columnMap[] = prop.split(",");
				String value = resultJn.at(columnMap[0]).get(quoteIndex).asText();
				value = columnMap.length == 3 ? adjustQuote(value, columnMap[2], quoteDivisor, quoteMultiplier) : value;
				returnRow.put(columnMap[1], value);
			}

		} catch (NumberFormatException e) {
			LOGGER.warn(e);
			LOGGER.debug("Exception occurred!", e);
		}

		quoteIndex++;
		return returnRow;
	}
}