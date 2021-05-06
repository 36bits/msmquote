package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;

class YahooApiHist extends QuoteSource {

	// Constants
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
	 * @param apiUrl		the Yahoo Finance quote history API URL
	 * @throws IOException
	 */
	YahooApiHist(String apiUrl) throws IOException {
		super(PROPS_FILE);

		// Get quote data
		resultJn = YahooUtil.getJson(apiUrl).at("/chart/result/0");

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
	 * @return	the quote row or null if no more data
	 */
	@Override
	Map<String, Object> getNext() {
		Map<String, Object> quoteRow = new HashMap<>();

		if (!resultJn.at("/timestamp").has(quoteIndex)) {
			return null;
		}

		// Get quote date
		LocalDateTime quoteDate = Instant.ofEpochSecond(resultJn.at("/timestamp").get(quoteIndex).asLong()).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();

		// Build columns for msmquote internal use
		quoteRow.put("xSymbol", symbol);

		// Build SEC table columns
		quoteRow.put("dtLastUpdate", quoteDate);			// TODO Confirm assumption that dtLastUpdate is date of quote data in SEC row

		// Build SP table columns
		quoteRow.put("dt", quoteDate);

		// SP table columns
		quoteRow.put("dt", quoteDate);
		try {
			String prop;
			String[] apiHistMap;
			double value;
			int n = 1;
			while ((prop = PROPS.getProperty("hist.api." + n++)) != null) {
				apiHistMap = prop.split(",");
				value = resultJn.at(apiHistMap[0]).get(quoteIndex).asDouble();
				// Process adjustments
				if (Boolean.parseBoolean(PROPS.getProperty("divide." + apiHistMap[1]))) {
					value = value / quoteDivisor;
				}
				// Now put key and value to quote row
				LOGGER.debug("Key = {}, value = {}", apiHistMap[1], value);
				if (apiHistMap[1].substring(0, 1).equals("d")) {
					quoteRow.put(apiHistMap[1], value);
				} else {
					quoteRow.put(apiHistMap[1], (long) value);
				}
			}

		} catch (NumberFormatException e) {
			LOGGER.warn(e);
			LOGGER.debug("Exception occured!", e);
			quoteRow.put("xError", null);
		}

		quoteIndex++;
		return quoteRow;
	}
}