package uk.co.pueblo.msmquote;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

public class YahooApiQuote extends YahooApiSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiQuote.class);
	private static final String JSON_ROOT = "/quoteResponse/result";

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, String> symbolMap = new HashMap<>();

	/**
	 * Constructor for auto-generated URL.
	 * 
	 * @param apiUrl     the base URL
	 * @param secSymbols the list of investment symbols + country codes
	 * @param isoCodes   the list of currency ISO codes, last element is base currency
	 * @throws APIException
	 */
	YahooApiQuote(String apiUrl, List<String> secSymbols, List<String> cntryCodes, List<String> crncPairs) throws APIException {

		String yahooSymbol = "";

		// Build Yahoo security symbols string
		String secSymbolsCsv = "";
		int n = 0;
		for (String secSymbol : secSymbols) {
			// Append the symbols pair to the symbol translation map and the Yahoo symbol to the investment symbols string
			yahooSymbol = getYahooSymbol(secSymbol, cntryCodes.get(n++));
			symbolMap.put(yahooSymbol, secSymbol);
			secSymbolsCsv = secSymbolsCsv + yahooSymbol + ",";
		}
		if (secSymbolsCsv.isEmpty()) {
			LOGGER.warn("No security symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with these security symbols: {}", secSymbolsCsv.substring(0, secSymbolsCsv.length() - 1));
		}

		// Build Yahoo currency symbols string
		String fxSymbolsCsv = "";
		for (String crncPair : crncPairs) {
			yahooSymbol = crncPair + "=X";
			symbolMap.put(yahooSymbol, crncPair);
			fxSymbolsCsv = fxSymbolsCsv + yahooSymbol + ",";
		}
		if (fxSymbolsCsv.isEmpty()) {
			LOGGER.warn("No FX symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with these FX symbols: {}", fxSymbolsCsv.substring(0, fxSymbolsCsv.length() - 1));
		}

		// Get quote data from api
		String allSymbols = secSymbolsCsv + fxSymbolsCsv;
		if (!apiUrl.endsWith("symbols=?") && !allSymbols.isEmpty()) {
			resultIt = getJson(apiUrl + allSymbols.substring(0, allSymbols.length() - 1)).at(JSON_ROOT).elements();
		}
	}

	/**
	 * Constructor for user-supplied URL.
	 * 
	 * @param apiUrl the complete Yahoo Finance quote API URL
	 * @throws APIException
	 */
	YahooApiQuote(String apiUrl) throws APIException {
		resultIt = getJson(apiUrl).at(JSON_ROOT).elements();
	}

	/**
	 * Gets the next row of quote data from the JSON iterator.
	 * 
	 * @return the quote row or null if no more data
	 */
	public Map<String, String> getNext() {
		// Get next JSON node from iterator
		if (resultIt == null || !resultIt.hasNext()) {
			return null;
		}

		JsonNode result = resultIt.next();
		Map<String, String> returnRow = new HashMap<>();

		try {
			// Add quote type to return row
			String quoteType = result.get("quoteType").asText();
			returnRow.put("xType", quoteType);

			// Add symbol to return row
			String yahooSymbol = result.get("symbol").asText();
			if (symbolMap.isEmpty()) {
				returnRow.put("xSymbol", yahooSymbol);
			} else {
				returnRow.put("xSymbol", symbolMap.get(yahooSymbol));
			}

			// Get divisor or multiplier for quote currency and quote type
			String quoteCurrency = result.get("currency").asText();
			String prop;
			int quoteDivisor = getDivisor(quoteCurrency, quoteType);
			int quoteMultiplier = getMultiplier(quoteCurrency, quoteType);

			// Add quote values to return row
			int n = 1;
			while ((prop = PROPS.getProperty("api." + quoteType + "." + n++)) != null) {
				String[] columnMap = prop.split(",");
				if (result.has(columnMap[0])) {
					String value = result.get(columnMap[0]).asText();
					value = columnMap.length == 3 ? adjustQuote(value, columnMap[2], quoteDivisor, quoteMultiplier) : value;
					returnRow.put(columnMap[1], value);
				}
			}

		} catch (NullPointerException e) {
			LOGGER.debug("Exception occurred!", e);
		}

		return returnRow;
	}
}