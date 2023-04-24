package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

public class YahooApiQuote extends YahooSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiQuote.class);
	private static final String JSON_ROOT = "/quoteResponse/result";
	private static final String PROPS_FILE = "YahooSource.properties";
	private static final String API_URL = "https://query2.finance.yahoo.com/v7/finance/quote?symbols=";
	
	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, String> symbolXlate = new HashMap<>();

	/**
	 * Constructor for auto-completed URL.
	 * 
	 * @param apiUrl   the base URL
	 * @param symbols  the list of investment symbols + country codes
	 * @param isoCodes the list of currency ISO codes, last element is base currency
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 * @throws APIErrorException 
	 */
	YahooApiQuote(String apiUrl, List<String[]> symbols, List<String> isoCodes) throws IOException, InterruptedException, URISyntaxException, APIErrorException {
		super(PROPS_FILE);
		
		// Get api url from properties file 
		if (apiUrl == null) {
			String prop;
			apiUrl = ((prop = PROPS.getProperty("api.url")) != null) ? prop : API_URL;
		}		

		String yahooSymbol = "";
		int n;

		// Build Yahoo security symbols string
		String invSymbols = "";
		String[] symbol = new String[2];
		for (n = 0; n < symbols.size(); n++) {
			// Append the symbols pair to the symbol translation table and the Yahoo symbol to the investment symbols string
			symbol = symbols.get(n);
			if ((yahooSymbol = getYahooSymbol(symbol[0], symbol[1], PROPS)) != null) {
				symbolXlate.put(yahooSymbol, symbol[0]);
				invSymbols = invSymbols + yahooSymbol + ",";
			}
		}
		if (invSymbols.isEmpty()) {
			LOGGER.warn("No security symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with these security symbols: {}", invSymbols.substring(0, invSymbols.length() - 1));
		}

		// Build Yahoo currency symbols string
		String baseIsoCode = null;
		String fxSymbols = "";
		int isoCodesSz = isoCodes.size();
		for (n = isoCodesSz; n > 0; n--) {
			if (n == isoCodesSz) {
				baseIsoCode = isoCodes.get(n - 1);
				continue;
			}
			// Append the symbols pair to the symbol translation table and to the FX symbols string
			yahooSymbol = baseIsoCode + isoCodes.get(n - 1) + "=X";
			symbolXlate.put(yahooSymbol, yahooSymbol);
			fxSymbols = fxSymbols + yahooSymbol + ",";
		}
		if (fxSymbols.isEmpty()) {
			LOGGER.warn("No FX symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with these FX symbols: {}", fxSymbols.substring(0, fxSymbols.length() - 1));
		}

		String allSymbols = invSymbols + fxSymbols;
		if (!apiUrl.endsWith("symbols=?") && !allSymbols.isEmpty()) {
			// Get quote data
			resultIt = getJson(apiUrl + allSymbols.substring(0, allSymbols.length() - 1)).at(JSON_ROOT).elements();
		}
	}

	/**
	 * Constructor for auto-completed default URL.
	 * 
	 * @param symbols  the list of investment symbols + country codes
	 * @param isoCodes the list of currency ISO codes, last element is base currency
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 * @throws APIErrorException 
	 */
	YahooApiQuote(List<String[]> symbols, List<String> isoCodes) throws IOException, InterruptedException, URISyntaxException, APIErrorException {
		this(null, symbols, isoCodes);
	}
	
	/**
	 * Constructor for user-completed URL.
	 * 
	 * @param apiUrl the complete Yahoo Finance quote API URL
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws URISyntaxException
	 * @throws APIErrorException 
	 */
	YahooApiQuote(String apiUrl) throws IOException, InterruptedException, URISyntaxException, APIErrorException {
		super(PROPS_FILE);
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
			if (symbolXlate.isEmpty()) {
				returnRow.put("xSymbol", yahooSymbol);
			} else {
				returnRow.put("xSymbol", symbolXlate.get(yahooSymbol));
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