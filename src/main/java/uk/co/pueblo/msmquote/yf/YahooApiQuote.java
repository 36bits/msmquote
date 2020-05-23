package uk.co.pueblo.msmquote.yf;

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

import uk.co.pueblo.msmquote.Quote;

public class YahooApiQuote implements Quote {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiQuote.class);
	private static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	private static final String DELIM = ",";
	private static final String PROPS = "YahooQuote.properties";
	private static final String JSON_ROOT = "/quoteResponse/result";

	// Class variables
	private static Properties props;

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, Integer> logSummary;
	private boolean isQuery;

	static {
		try {
			// Set up properties
			InputStream propsIs = YahooApiQuote.class.getClassLoader().getResourceAsStream(PROPS);
			props = new Properties();
			props.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
	}

	// Define Yahoo quoteType fields that we recognise
	private enum QuoteType {
		EQUITY, BOND, MUTUALFUND, INDEX, CURRENCY
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
				symbol[0] = symbol[0] + props.getProperty("exchange." + symbol[1]);
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
		int quoteDivisor = 1;

		try {
			// Get quote type
			symbol = result.get("symbol").asText();
			quoteType = result.get("quoteType").asText();
			LOGGER.info("Processing quote data for symbol {}, quote type = {}", symbol, quoteType);

			if (quoteType.equals(QuoteType.CURRENCY.toString())) {
				quoteRow.put("symbol", result.get("symbol").asText());
				quoteRow.put("rate", result.get("regularMarketPrice").asDouble());
			} else {
				// Get quote date set to 00:00 in local system time-zone
				LocalDateTime quoteDate = Instant.ofEpochSecond(result.get("regularMarketTime").asLong()).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();

				// Set conversion factor for quote currency
				if (quoteType.equals(QuoteType.EQUITY.toString()) || quoteType.equals(QuoteType.BOND.toString())
						|| quoteType.equals(QuoteType.MUTUALFUND.toString())) {
					String quoteDivisorProp = props.getProperty("quoteDivisor." + result.get("currency").asText());
					if (quoteDivisorProp != null) {
						quoteDivisor = Integer.parseInt(quoteDivisorProp);
					}
				}

				// Build quote row
				// Columns common to EQUITY, BOND, MUTUALFUND and INDEX quote types
				// SEC and SP table
				quoteRow.put("dtSerial", LocalDateTime.now()); // TODO Confirm assumption that dtSerial is time-stamp of
				// record creation/update
				// SEC table
				quoteRow.put("szSymbol", symbol);
				quoteRow.put("dtLastUpdate", quoteDate); // TODO Confirm assumption that dtLastUpdate is date of quote
				// data in SEC row
				quoteRow.put("d52WeekLow", result.get("fiftyTwoWeekLow").asDouble() / quoteDivisor);
				quoteRow.put("d52WeekHigh", result.get("fiftyTwoWeekHigh").asDouble() / quoteDivisor);
				// SP table
				quoteRow.put("dt", quoteDate);
				quoteRow.put("dPrice", result.get("regularMarketPrice").asDouble() / quoteDivisor);
				quoteRow.put("dChange", result.get("regularMarketChange").asDouble() / quoteDivisor);

				// Columns common to EQUITY, BOND and INDEX quote types
				if (quoteType.equals(QuoteType.EQUITY.toString()) || quoteType.equals(QuoteType.BOND.toString())
						|| quoteType.equals(QuoteType.INDEX.toString())) {
					// SEC table
					quoteRow.put("dBid", result.get("bid").asDouble() / quoteDivisor); // Not visible in Money 2004
					quoteRow.put("dAsk", result.get("ask").asDouble() / quoteDivisor); // Not visible in Money 2004
					// SP table
					quoteRow.put("dOpen", result.get("regularMarketOpen").asDouble() / quoteDivisor);
					quoteRow.put("dHigh", result.get("regularMarketDayHigh").asDouble() / quoteDivisor);
					quoteRow.put("dLow", result.get("regularMarketDayLow").asDouble() / quoteDivisor);
					quoteRow.put("vol", result.get("regularMarketVolume").asLong());
				}

				// Columns for EQUITY quote types
				if (quoteType.equals(QuoteType.EQUITY.toString())) {
					// SEC table
					// TODO Add EPS and beta
					quoteRow.put("dCapitalization", result.get("marketCap").asDouble());
					quoteRow.put("dSharesOutstanding", result.get("sharesOutstanding").asDouble());
					if (result.has("trailingAnnualDividendYield")) {
						quoteRow.put("dDividendYield",
								result.get("trailingAnnualDividendYield").asDouble() * 100 * quoteDivisor);
					} else {
						quoteRow.put("dDividendYield", 0);
					}
					// SP table
					if (result.has("trailingPE")) {
						quoteRow.put("dPE", result.get("trailingPE").asDouble());
					}
				}
			}
		} catch (NullPointerException e) {
			LOGGER.warn("Incomplete quote data for symbol {}", symbol);
			LOGGER.debug("Exception occured!", e);
			quoteRow.put("xError", null);
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