package uk.co.pueblo.msmquote;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
	 * @param baseUrl     the base URL for the Yahoo Finance API
	 * @param secSymbols the list of Money security symbols and associated country codes for which to get quotes
	 * @param crncPairs  the list of Money currency pairs for which to get quotes
	 * @throws QuoteSourceException
	 */
	public YahooApiQuote(String baseUrl, List<String[]> secSymbols, List<String[]> crncPairs) throws QuoteSourceException {

		String yahooSymbol = "";

		// Build Yahoo security symbols string
		StringJoiner secSymbolsSj = new StringJoiner(",");
		int i = 0;
		for (String secSymbol[] : secSymbols) {
			// Append the symbols pair to the symbol translation map and the Yahoo symbol to the investment symbols string joiner
			if ((yahooSymbol = getYahooSymbol(secSymbol[0], secSymbol[1])) == null) {
				LOGGER.warn("Cannot find Yahoo Finance exchange suffix for symbol {}, country code={}", secSymbol[0], secSymbol[1]);
				sourceStatus = SourceStatus.WARN;
				yahooSymbol = secSymbol[0];
			}
			symbolMap.put(yahooSymbol, secSymbol[0]);
			secSymbolsSj.add(yahooSymbol);
			i++;
		}
		if (i == 0) {
			LOGGER.warn("No security symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with {} security {}: {}", i, i == 1 ? "symbol" : "symbols", secSymbolsSj.toString());
		}

		// Build Yahoo currency symbols string
		StringJoiner fxSymbolsSj = new StringJoiner(",");
		i = 0;
		for (String crncPair[] : crncPairs) {
			yahooSymbol = crncPair[0];
			symbolMap.put(yahooSymbol, crncPair[0]);
			fxSymbolsSj.add(yahooSymbol);
			i++;
		}
		if (i == 0) {
			LOGGER.warn("No FX symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with {} FX {}: {}", i, i == 1 ? "symbol" : "symbols", fxSymbolsSj.toString());
		}

		// Get quote data from api
		String allSymbols = URLEncoder.encode(secSymbolsSj.merge(fxSymbolsSj).toString(), StandardCharsets.UTF_8);
		if (!baseUrl.endsWith("symbols=?") && !allSymbols.isEmpty()) {			
			boolean loop = true;
			i = 1;
			while (loop) {
				String fullUrl;
				if (baseUrl.endsWith("symbols=")) {
					fullUrl = baseUrl + allSymbols; 
					loop = false;
				} else if ((baseUrl = PROPS.getProperty("api.url." + i)) == null) {
					break;
				} else { 
					fullUrl = baseUrl + "symbols=" + allSymbols;
				}
				LOGGER.info("Trying Yahoo Finance API url #{}", i++);
				JsonNode jn;
				if ((jn = getJson(fullUrl)) == null) {
					sourceStatus = SourceStatus.WARN;
				} else {
					resultIt = jn.at(JSON_ROOT).elements();
					return;
				}
			}
			throw new QuoteSourceException("All Yahoo Finance API requests failed!");
		}
	}

	/**
	 * Constructs a Yahoo Finance API quote source from a user-supplied URL.
	 * 
	 * @param apiUrl the complete URL for the Yahoo Finance API
	 * @throws QuoteSourceException
	 */
	public YahooApiQuote(String apiUrl) throws QuoteSourceException {
		JsonNode jn;
		if ((jn = getJson(apiUrl)) != null) {
			resultIt = jn.at(JSON_ROOT).elements();
		} else {
			throw new QuoteSourceException("All Yahoo Finance API requests failed!");
		}
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

			// Get quote adjuster for currency and quote type
			int quoteAdjuster = getAdjuster(PROPS, result.get("currency").asText(), result.get("quoteType").asText());

			// Add quote values to return row
			String prop;
			int n = 1;
			while ((prop = PROPS.getProperty("api." + quoteType + '.' + n++)) != null) {
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