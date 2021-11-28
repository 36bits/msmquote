package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

class YahooApiHist extends QuoteSource {

	// Constants
	static final Logger LOGGER = LogManager.getLogger(YahooApiHist.class);
	private static final String PROPS_FILE = "YahooSource.properties";

	// Instance variables
	private JsonNode resultJn;
	private String symbol;
	private int quoteDivisor;
	private int quoteIndex;
	private String quoteType;

	/**
	 * Constructor.
	 * 
	 * @param apiUrl the URL of the Yahoo Finance quote history API
	 * @throws IOException
	 */
	YahooApiHist(String apiUrl) throws IOException {
		super(PROPS_FILE);

		// Get quote data
		resultJn = getJson(apiUrl).at("/chart/result/0");

		// Get symbol and quote divisor
		quoteDivisor = 1;
		symbol = resultJn.at("/meta").get("symbol").asText();
		quoteType = resultJn.at("/meta").get("instrumentType").asText();
		String quoteDivisorProp = PROPS.getProperty("divisor." + resultJn.at("/meta").get("currency").asText() + "." + quoteType);
		if (quoteDivisorProp != null) {
			quoteDivisor = Integer.parseInt(quoteDivisorProp);
		}

		quoteIndex = 0;
	}

	/**
	 * Gets the next row of quote data from the JSON node.
	 * 
	 * @return the quote row or null if no more data
	 */
	@Override
	Map<String, Object> getNext() {
		Map<String, Object> returnRow = new HashMap<>();

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
			String[] apiHistMap;
			Double dValue = 0d;
			LocalDateTime dtValue;
			while ((prop = PROPS.getProperty("hist.api." + n++)) != null) {
				apiHistMap = prop.split(",");
				if (apiHistMap[0].startsWith("dt")) {
					// Process LocalDateTime values
					dtValue = Instant.ofEpochSecond(resultJn.at(apiHistMap[1]).get(quoteIndex).asLong()).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();
					returnRow.put(apiHistMap[0], dtValue);
				} else if (apiHistMap[0].startsWith("d")) {
					// Process Double values
					dValue = resultJn.at(apiHistMap[1]).get(quoteIndex).asDouble();
					// Process adjustments
					if (Boolean.parseBoolean(PROPS.getProperty("divide." + apiHistMap[0]))) {
						dValue = dValue / quoteDivisor;
					}
					returnRow.put(apiHistMap[0], dValue);
				} else {
					// And finally process Long values
					returnRow.put(apiHistMap[0], resultJn.at(apiHistMap[1]).get(quoteIndex).asLong());
				}
			}

		} catch (NumberFormatException e) {
			LOGGER.warn(e);
			LOGGER.debug("Exception occurred!", e);
		}

		quoteIndex++;
		return returnRow;
	}
}