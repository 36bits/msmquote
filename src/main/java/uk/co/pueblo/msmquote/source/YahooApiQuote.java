package uk.co.pueblo.msmquote.source;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

public class YahooApiQuote implements Quote {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiQuote.class);
	private static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	private static final String DELIM = ",";
	private static final String BASE_PROPS = "YahooQuote.properties";
	private static final String JSON_ROOT = "/quoteResponse/result";

	// Class variables
	private static Properties baseProps;

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, Integer> logSummary;
	private boolean isQuery;

	static {
		try {
			// Set up base properties			
			InputStream propsIs = YahooApiQuote.class.getClassLoader().getResourceAsStream(BASE_PROPS);
			baseProps = new Properties();
			baseProps.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
	}

	/**
	 * Constructor for auto-completed URL.
	 * 
	 * @param base URL
	 * @param list of investment symbols + country codes
	 * @param list of currency ISO codes, last element is base currency
	 * @throws IOException
	 */
	public YahooApiQuote(String apiUrl, List<String[]> symbols, List<String> isoCodes) throws IOException {

		// Build Yahoo investment symbols string
		String invSymbols = "";
		int n;
		String delim;
		String[] symbol = new String[2];
		for (n = 0; n < symbols.size(); n++) {
			symbol = symbols.get(n);
			if (symbol[0].length() == 12 && !symbol[0].contains(".")) {
				symbol[0] = symbol[0] + baseProps.getProperty("exchange." + symbol[1]);
			}
			delim = DELIM;
			if (n == 0) {
				delim = "";
			}
			invSymbols = invSymbols + delim + symbol[0];
		}
		LOGGER.info("Building URL with these stock symbols: {}", invSymbols);

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
			fxSymbols = fxSymbols + delim + baseIsoCode + isoCodes.get(n - 1) + "=X";
		}
		LOGGER.info("Building URL with these FX symbols: {}", fxSymbols);

		if (apiUrl.endsWith("symbols=?")) {
			isQuery = true;
			return;
		}

		// Append symbols to Yahoo API URL
		delim = DELIM;
		if (invSymbols.isEmpty()) {
			delim = "";
		}

		isQuery = false;
		logSummary = new HashMap<>();
		resultIt = YahooApi.getJson(apiUrl + invSymbols + delim + fxSymbols).at(JSON_ROOT).elements();
	}

	/**
	 * Constructor for user-completed URL.
	 * 
	 * @param apiUrl
	 * @throws IOException
	 */
	public YahooApiQuote(String apiUrl) throws IOException {
		isQuery = false;
		logSummary = new HashMap<>();
		resultIt = YahooApi.getJson(apiUrl).at(JSON_ROOT).elements();
	}

	/**
	 * Get the next row of quote data from the JSON iterator.
	 * 
	 * @return
	 */
	@Override
	public Map<String, Object> getNext() {
		// Get next JSON node from iterator
		if (!resultIt.hasNext()) {
			logSummary.forEach((key, value) -> LOGGER.info("Summary for quote type {}: processed = {}", key, value));
			return null;
		}
		JsonNode result = resultIt.next();

		Map<String, Object> quoteRow = new HashMap<>();
		String symbol = null;
		String quoteType = null;


		// Get quote type
		symbol = result.get("symbol").asText();
		quoteType = result.get("quoteType").asText();
		LOGGER.info("Processing quote data for symbol {}, quote type = {}", symbol, quoteType);

		// Set quote date to 00:00 in local system time-zone
		LocalDateTime quoteDate = Instant.ofEpochSecond(result.get("regularMarketTime").asLong()).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();

		// Get divisor and multiplier for quote currency
		String quoteCurrency = result.get("currency").asText();
		String prop;
		int quoteDivisor = 1;
		int quoteMultiplier = 100;
		if ((prop = baseProps.getProperty("divisor." + quoteCurrency)) != null) {
			quoteDivisor = Integer.parseInt(prop);
		}
		if ((prop = baseProps.getProperty("multiplier." + quoteCurrency)) != null) {
			quoteMultiplier = Integer.parseInt(prop);				
		}

		// Build columns common to SEC and SP tables
		quoteRow.put("dtSerial", LocalDateTime.now());	// TODO Confirm assumption that dtSerial is time-stamp of quote

		// Build SEC table columns
		quoteRow.put("xSymbol", symbol);				// xSymbol is used internally, not by MS Money
		quoteRow.put("dtLastUpdate", quoteDate);		// TODO Confirm assumption that dtLastUpdate is date of quote
		
		// Build SP table columns				
		quoteRow.put("dt", quoteDate);

		// Build remaining columns
		int n = 1;
		while ((prop = baseProps.getProperty("map." + quoteType + "." + n++)) != null) {
			String[] map = prop.split(",");
			double value;
			if (result.has(map[0])) {
				value = result.get(map[0]).asDouble();
				// Process adjustments
				if ((prop = baseProps.getProperty("adjust." + map[0])) != null) {
					switch(prop) {
					case "divide":
						value = value / quoteDivisor;
						break;
					case "multiply":
						value = value * quoteMultiplier;
					}
				}
			} else {
				LOGGER.warn("Incomplete quote data for symbol {}, missing = {}", symbol, map[0]);
				quoteRow.put("xError", null);
				if ((prop = baseProps.getProperty("default." + map[0])) == null) {
					continue;
				}
				value = Double.parseDouble(prop);	// Get default value
			}

			// Now put key and value to quote row
			LOGGER.debug("Key = {}, value = {}", map[1], value);
			if (map[1].substring(0, 1).equals("d")) {
				quoteRow.put(map[1], value);
			} else {
				quoteRow.put(map[1], (long) value);
			}
		}

		logSummary.putIfAbsent(quoteType, 0);
		logSummary.put(quoteType, logSummary.get(quoteType) + 1);

		return quoteRow;
	}

	/**
	 * 
	 * 
	 * @return
	 */
	@Override
	public boolean isQuery() {
		return isQuery;
	}
}