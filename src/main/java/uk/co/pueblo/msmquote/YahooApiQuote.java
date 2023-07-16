package uk.co.pueblo.msmquote;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A Yahoo Finance API quote source.
 */
public class YahooApiQuote extends YahooApiSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiQuote.class);
	private static final String JSON_ROOT = "/quoteResponse/result";

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, String> symbolMap = new HashMap<>();

	/**
	 * Constructs a Yahoo Finance API quote source using an auto-generated URL.
	 * 
	 * @param apiUrl     the base URL for the Yahoo Finance API
	 * @param secSymbols the list of Money security symbols for which to get quotes
	 * @param cntryCodes the corresponding list of Money country codes for each security symbol
	 * @param crncPairs  the list of Money currency pairs for which to get quotes
	 */
	public YahooApiQuote(String apiUrl, List<String> secSymbols, List<String> cntryCodes, List<String> crncPairs) throws APIException {

		String yahooSymbol = "";
		
		// Build Yahoo security symbols string
		StringJoiner secSymbolsSj = new StringJoiner(",");
		int i = 0;
		for (String secSymbol : secSymbols) {
			// Append the symbols pair to the symbol translation map and the Yahoo symbol to the investment symbols string
			yahooSymbol = getYahooSymbol(secSymbol, cntryCodes.get(i++));
			symbolMap.put(yahooSymbol, secSymbol);
			secSymbolsSj.add(yahooSymbol);
		}
		if (i == 0) {
			LOGGER.warn("No security symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with {} security {}: {}", i, i == 1 ? "symbol" : "symbols", secSymbolsSj.toString());
		}

		// Build Yahoo currency symbols string
		StringJoiner fxSymbolsSj = new StringJoiner(",");
		i = 0;
		for (String crncPair : crncPairs) {
			yahooSymbol = crncPair + "=X";
			symbolMap.put(yahooSymbol, crncPair);
			fxSymbolsSj.add(yahooSymbol);
			i++;
		}
		if (i == 0) {
			LOGGER.warn("No FX symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with {} FX {}: {}", i, i == 1 ? "symbol" : "symbols", fxSymbolsSj.toString());
		}

		// Get quote data from api
		String allSymbolsCsv = secSymbolsSj.merge(fxSymbolsSj).toString();
		if (!apiUrl.endsWith("symbols=?") && !allSymbolsCsv.isEmpty()) {
			resultIt = getJson(apiUrl + allSymbolsCsv).at(JSON_ROOT).elements();
		}
	}

	/**
	 * Constructs a Yahoo Finance API quote source from a user-supplied URL.
	 * 
	 * @param apiUrl the complete URL for the Yahoo Finance API
	 */
	public YahooApiQuote(String apiUrl) throws APIException {
		resultIt = getJson(apiUrl).at(JSON_ROOT).elements();
	}

	
	public Map<String, String> getNext() {
		// Get next JSON node from iterator
		Map<String, String> returnRow = new HashMap<>();
		if (resultIt == null || !resultIt.hasNext()) {
			return returnRow;
		}
		JsonNode result = resultIt.next();

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

			// Get quote adjuster for currency
			int quoteAdjuster = getAdjuster(result.get("currency").asText());

			// Add quote values to return row
			String prop;
			int n = 1;
			while ((prop = PROPS.getProperty("api." + quoteType + "." + n++)) != null) {
				String[] columnMap = prop.split(",");
				if (result.has(columnMap[0])) {
					String value = result.get(columnMap[0]).asText();
					value = columnMap.length == 3 ? adjustQuote(value, columnMap[2], quoteAdjuster) : value;
					returnRow.put(columnMap[1], value);
				}
			}

		} catch (NullPointerException e) {
			LOGGER.debug("Exception occurred!", e);
		}

		return returnRow;
	}
}