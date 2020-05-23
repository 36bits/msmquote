package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

public class YahooApiHist implements YahooUpdate {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiHist.class);
	private static final ZoneId SYS_ZONE_ID = ZoneId.systemDefault();
	private static final String PROPS = "YahooQuote.properties";

	// Class variables
	private static Properties props;

	// Instance variables
	private JsonNode resultJn;
	private String symbol;
	private int quoteDivisor;
	private int quoteIndex;
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

	/**
	 * Constructor. 
	 * 
	 * @param apiUrl
	 * @throws IOException
	 */
	public YahooApiHist(String apiUrl) throws IOException {

		// Get quote data
		resultJn = YahooApi.getJson(apiUrl).at("/chart/result/0");

		// Get symbol and quote divisor
		quoteDivisor = 1;
		symbol = resultJn.at("/meta").get("symbol").asText();
		String quoteDivisorProp = props.getProperty("quoteDivisor." + resultJn.at("/meta").get("currency").asText());
		if (quoteDivisorProp != null) {
			quoteDivisor = Integer.parseInt(quoteDivisorProp);
		}

		isQuery = false;
		quoteIndex = 0;

	}

	@Override
	public Map<String, Object> getNext() throws IOException {
		Map<String, Object> quoteRow = new HashMap<>();

		if (!resultJn.at("/timestamp").has(quoteIndex)) {
			LOGGER.info("Quotes processed = {}", quoteIndex);
			return null;
		}

		// Get quote date
		LocalDateTime quoteDate = Instant.ofEpochSecond(resultJn.at("/timestamp").get(quoteIndex).asLong()).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();

		// SEC table columns
		quoteRow.put("szSymbol", symbol);
		// Assume dtLastUpdate is date of quote data in SEC row
		quoteRow.put("dtLastUpdate", quoteDate);

		// SP table columns
		quoteRow.put("dt", quoteDate);
		quoteRow.put("dOpen", resultJn.at("/indicators/quote/0/open").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("dHigh", resultJn.at("/indicators/quote/0/high").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("dLow", resultJn.at("/indicators/quote/0/low").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("dPrice", resultJn.at("/indicators/quote/0/close").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("vol", resultJn.at("/indicators/quote/0/volume").get(quoteIndex).asLong());

		quoteIndex++;
		return quoteRow;
	}

	@Override
	public boolean isQuery() {
		return isQuery;
	}
}