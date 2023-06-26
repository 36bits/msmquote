package uk.co.pueblo.msmquote;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

class YahooApiHist extends YahooApiSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooApiHist.class);

	// Instance variables
	private JsonNode resultJn;
	private String symbol;
	private int quoteAdjuster;
	private int quoteIndex = 0;
	private String quoteType;

	/**
	 * Constructor.
	 * 
	 * @param apiUrl the URL of the Yahoo Finance quote history API
	 * @throws APIException
	 */
	YahooApiHist(String apiUrl) throws APIException {

		// Get symbol and quote type
		resultJn = getJson(apiUrl).at("/chart/result/0");
		symbol = resultJn.at("/meta").get("symbol").asText();
		quoteType = resultJn.at("/meta").get("instrumentType").asText();

		// Get quote adjuster for currency
		quoteAdjuster = getAdjuster(resultJn.at("/meta").get("currency").asText());
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
				value = columnMap.length == 3 ? adjustQuote(value, columnMap[2], quoteAdjuster) : value;
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