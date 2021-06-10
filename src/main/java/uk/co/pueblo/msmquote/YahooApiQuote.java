package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;

public class YahooApiQuote extends QuoteSource {

	// Constants
	private static final String DELIM = ",";
	private static final String JSON_ROOT = "/quoteResponse/result";
	private static final String PROPS_FILE = "YahooSource.properties";

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, String> symbolXlate;
	private boolean useXlate;

	/**
	 * Constructor for auto-completed URL.
	 * 
	 * @param apiUrl   the base URL
	 * @param symbols  the list of investment symbols + country codes
	 * @param isoCodes the list of currency ISO codes, last element is base currency
	 * @throws IOException
	 */
	YahooApiQuote(String apiUrl, List<String[]> symbols, List<String> isoCodes) throws IOException {
		super(PROPS_FILE);

		symbolXlate = new HashMap<>();
		useXlate = true;
		String yahooSymbol = "";
		String delim;
		int n;

		// Build Yahoo security symbols string
		String invSymbols = "";
		String[] symbol = new String[2];
		for (n = 0; n < symbols.size(); n++) {
			// Append the symbols pair to the symbol translation table and the Yahoo symbol
			// to the investment symbols string
			symbol = symbols.get(n);
			if ((yahooSymbol = YahooUtil.getYahooSymbol(symbol[0], symbol[1], PROPS)) != null) {
				symbolXlate.put(yahooSymbol, symbol[0]);
				delim = DELIM;
				if (n == 0) {
					delim = "";
				}
				invSymbols = invSymbols + delim + yahooSymbol;
			}
		}
		LOGGER.info("Building URL with these security symbols: {}", invSymbols);

		// Build Yahoo currency symbols string
		String baseIsoCode = null;
		String fxSymbols = "";
		int isoCodesSz = isoCodes.size();
		for (n = isoCodesSz; n > 0; n--) {
			if (n == isoCodesSz) {
				baseIsoCode = isoCodes.get(n - 1);
				continue;
			}
			delim = DELIM;
			if (n == isoCodesSz - 1) {
				delim = "";
			}
			// Append the symbols pair to the symbol translation table and to the FX symbols
			// string
			yahooSymbol = baseIsoCode + isoCodes.get(n - 1) + "=X";
			symbolXlate.put(yahooSymbol, yahooSymbol);
			fxSymbols = fxSymbols + delim + yahooSymbol;
		}
		LOGGER.info("Building URL with these FX symbols: {}", fxSymbols);

		if (apiUrl.endsWith("symbols=?")) {
			isQuery = true;
			return;
		}

		// Generate delimiter for FX symbols string
		delim = DELIM;
		if (invSymbols.isEmpty()) {
			delim = "";
		}

		resultIt = YahooUtil.getJson(apiUrl + invSymbols + delim + fxSymbols).at(JSON_ROOT).elements();
	}

	/**
	 * Constructor for user-completed URL.
	 * 
	 * @param apiUrl the complete Yahoo Finance quote API URL
	 * @throws IOException
	 */
	YahooApiQuote(String apiUrl) throws IOException {
		super(PROPS_FILE);
		useXlate = false;
		symbolXlate = new HashMap<>();
		resultIt = YahooUtil.getJson(apiUrl).at(JSON_ROOT).elements();
	}

	/**
	 * Gets the next row of quote data from the JSON iterator.
	 * 
	 * @return the quote row or null if no more data
	 */
	@Override
	Map<String, Object> getNext() {
		// Get next JSON node from iterator
		if (!resultIt.hasNext()) {
			return null;
		}

		JsonNode result = resultIt.next();
		Map<String, Object> returnRow = new HashMap<>();
		String yahooSymbol = null;

		try {
			// Get quote type
			yahooSymbol = result.get("symbol").asText();
			String quoteType = result.get("quoteType").asText();

			// Get divisor and multiplier for quote currency
			String quoteCurrency = result.get("currency").asText();
			String prop;
			int quoteDivisor = 1;
			int quoteMultiplier = 100;
			if ((prop = PROPS.getProperty("divisor." + quoteCurrency + "." + quoteType)) != null) {
				quoteDivisor = Integer.parseInt(prop);
			}
			if ((prop = PROPS.getProperty("multiplier." + quoteCurrency + "." + quoteType)) != null) {
				quoteMultiplier = Integer.parseInt(prop);
			}

			// Add quote symbol and type values to return row
			if (useXlate) {
				returnRow.put("xSymbol", symbolXlate.get(yahooSymbol));
			} else {
				returnRow.put("xSymbol", yahooSymbol);
			}
			returnRow.put("xType", quoteType);

			// Add quote values to return row
			int n = 1;
			Double dValue = 0d;
			LocalDateTime dtValue;
			while ((prop = PROPS.getProperty("api." + quoteType + "." + n++)) != null) {
				String[] apiMap = prop.split(",");
				if (result.has(apiMap[1])) {
					if (apiMap[0].startsWith("dt")) {
						// Process LocalDateTime values
						dtValue = Instant.ofEpochSecond(result.get(apiMap[1]).asLong()).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay(); // Set to 00:00 in local system time-zone
						returnRow.put(apiMap[0], dtValue);
					} else if (apiMap[0].startsWith("d")) {
						// Process Double values
						dValue = result.get(apiMap[1]).asDouble();
						// Process adjustments
						if (Boolean.parseBoolean(PROPS.getProperty("divide." + apiMap[0]))) {
							dValue = dValue / quoteDivisor;
						} else if ((Boolean.parseBoolean(PROPS.getProperty("multiply." + apiMap[0])))) {
							dValue = dValue * quoteMultiplier;
						}
						returnRow.put(apiMap[0], dValue);
					} else {
						// And finally process Long values
						returnRow.put(apiMap[0], result.get(apiMap[1]).asLong());
					}
				}
			}

		} catch (NullPointerException e) {
			LOGGER.warn("Incomplete quote data for symbol {}", yahooSymbol);
			LOGGER.debug("Exception occurred!", e);
			returnRow.put("xWarn", null);
		}

		return returnRow;
	}
}