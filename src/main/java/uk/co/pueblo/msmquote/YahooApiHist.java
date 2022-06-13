package uk.co.pueblo.msmquote;

import java.io.IOException;
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
	 */
	YahooApiHist(String apiUrl) throws IOException, InterruptedException {
		super(PROPS_FILE);

		// Get symbol and quote type
		resultJn = getJson(apiUrl).at("/chart/result/0");
		symbol = resultJn.at("/meta").get("symbol").asText();
		quoteType = resultJn.at("/meta").get("instrumentType").asText();
		
		// Get divisor or multiplier for quote currency and quote type
		String currency = resultJn.at("/meta").get("currency").asText();
		String prop;
		quoteDivisor = ((prop = PROPS.getProperty("divisor." + currency + "." + quoteType)) == null) ? 1 : Integer.parseInt(prop);
		quoteMultiplier = ((prop = PROPS.getProperty("multiplier." + currency + "." + quoteType)) == null) ? 100 : Integer.parseInt(prop);
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
				String apiHistMap[] = prop.split(",");
				String value = resultJn.at(apiHistMap[0]).get(quoteIndex).asText();
				if (apiHistMap.length == 3) {
					if (apiHistMap[2].equals("d")) {
						value = String.valueOf(Double.parseDouble(value) / quoteDivisor);
					} else if (apiHistMap[2].equals("m")) {
						value = String.valueOf(Double.parseDouble(value) * quoteMultiplier);
					}
				}
				returnRow.put(apiHistMap[1], value);
			}

		} catch (NumberFormatException e) {
			LOGGER.warn(e);
			LOGGER.debug("Exception occurred!", e);
		}

		quoteIndex++;
		return returnRow;
	}
}