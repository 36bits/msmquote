package uk.co.pueblo.msmquote.source;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;

import uk.co.pueblo.msmquote.source.QuoteSummary.SummaryType;

public class YahooApiQuote extends YahooQuote {

	// Constants
	private static final String DELIM = ",";
	private static final String JSON_ROOT = "/quoteResponse/result";

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, String> symbolXlate;
	boolean useXlate;

	/**
	 * Constructor for auto-completed URL.
	 * 
	 * @param apiUrl	the base URL
	 * @param symbols	the list of investment symbols + country codes
	 * @param isoCodes	the list of currency ISO codes, last element is base currency
	 * @throws IOException
	 */
	public YahooApiQuote(String apiUrl, List<String[]> symbols, List<String> isoCodes) throws IOException {

		symbolXlate = new HashMap<>();
		useXlate = true;
		String yahooSymbol = null;
		String delim;
		int n;

		// Build Yahoo investment symbols string
		String invSymbols = "";
		String[] symbol = new String[2];
		for (n = 0; n < symbols.size(); n++) {
			// Append the symbols pair to the symbol translation table and the Yahoo symbol to the investment symbols string
			symbol = symbols.get(n);
			if ((yahooSymbol = YahooUtil.getYahooSymbol(symbol[0], symbol[1], baseProps)) != null) {
				symbolXlate.put(yahooSymbol, symbol[0]);
				delim = DELIM;
				if (n == 0) {
					delim = "";
				}
				invSymbols = invSymbols + delim + yahooSymbol;
			}
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
			// Append the symbols pair to the symbol translation table and to the FX symbols string
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

		isQuery = false;
		quoteSummary = new QuoteSummary();
		resultIt = YahooUtil.getJson(apiUrl + invSymbols + delim + fxSymbols).at(JSON_ROOT).elements();
	}

	/**
	 * Constructor for user-completed URL.
	 * 
	 * @param apiUrl		the complete Yahoo Finance quote API URL
	 * @throws IOException
	 */
	public YahooApiQuote(String apiUrl) throws IOException {
		isQuery = false;
		useXlate = false;
		quoteSummary = new QuoteSummary();
		symbolXlate = new HashMap<>();
		resultIt = YahooUtil.getJson(apiUrl).at(JSON_ROOT).elements();
	}

	/**
	 * Gets the next row of quote data from the JSON iterator.
	 * 
	 * @return	the quote row or null if no more data	
	 */
	@Override
	public Map<String, Object> getNext() {
		// Get next JSON node from iterator
		if (!resultIt.hasNext()) {
			quoteSummary.log(LOGGER);
			return null;
		}
		JsonNode result = resultIt.next();

		Map<String, Object> quoteRow = new HashMap<>();
		String yahooSymbol = null;
		String quoteType = null;

		try {
			// Get quote type
			yahooSymbol = result.get("symbol").asText();
			quoteType = result.get("quoteType").asText();
			LOGGER.info("Processing quote data for symbol {}, quote type = {}", yahooSymbol, quoteType);

			// Set quote date to 00:00 in local system time-zone
			LocalDateTime quoteDate = Instant.ofEpochSecond(result.get("regularMarketTime").asLong()).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();

			// Get divisor and multiplier for quote currency
			String quoteCurrency = result.get("currency").asText();
			String prop;
			int quoteDivisor = 1;
			int quoteMultiplier = 100;
			if ((prop = baseProps.getProperty("divisor." + quoteCurrency + "." + quoteType)) != null) {
				quoteDivisor = Integer.parseInt(prop);
			}
			if ((prop = baseProps.getProperty("multiplier." + quoteCurrency + "." + quoteType)) != null) {
				quoteMultiplier = Integer.parseInt(prop);				
			}

			// Build columns for msmquote internal use
			if (useXlate) {
				quoteRow.put("xSymbol", symbolXlate.get(yahooSymbol));
			} else {
				quoteRow.put("xSymbol", yahooSymbol);
			}

			// Build columns common to SEC and SP tables
			quoteRow.put("dtSerial", LocalDateTime.now());	// TODO Confirm assumption that dtSerial is time-stamp of quote

			// Build SEC table columns
			quoteRow.put("dtLastUpdate", quoteDate);		// TODO Confirm assumption that dtLastUpdate is date of quote

			// Build SP table columns				
			quoteRow.put("dt", quoteDate);

			// Build remaining columns
			int n = 1;
			while ((prop = baseProps.getProperty("api." + quoteType + "." + n++)) != null) {
				String[] apiMap = prop.split(",");
				double value;
				if (result.has(apiMap[0])) {
					value = result.get(apiMap[0]).asDouble();
					// Process adjustments
					if (Boolean.parseBoolean(baseProps.getProperty("divide." + apiMap[1]))) {
						value = value / quoteDivisor;
					} else if ((Boolean.parseBoolean(baseProps.getProperty("mutiply." + apiMap[1])))) {
						value = value * quoteMultiplier;
					}
				} else {
					LOGGER.warn("Incomplete quote data for symbol {}, missing = {}", yahooSymbol, apiMap[0]);
					quoteRow.put("xError", null);
					quoteSummary.inc(quoteType, SummaryType.WARNING);
					if ((prop = baseProps.getProperty("default." + apiMap[0])) == null) {
						continue;
					}
					value = Double.parseDouble(prop);	// Get default value
				}

				// Now put key and value to quote row
				LOGGER.debug("Key = {}, value = {}", apiMap[1], value);
				if (apiMap[1].substring(0, 1).equals("d")) {
					quoteRow.put(apiMap[1], value);
				} else {
					quoteRow.put(apiMap[1], (long) value);
				}
			}

		} catch (NullPointerException e) {
			LOGGER.warn("Incomplete quote data for symbol {}", yahooSymbol);
			LOGGER.debug("Exception occured!", e);
			quoteRow.put("xError", null);
			quoteSummary.inc(quoteType, SummaryType.WARNING);
		} finally {
			quoteSummary.inc(quoteType, SummaryType.PROCESSED);
		}

		return quoteRow;
	}
}