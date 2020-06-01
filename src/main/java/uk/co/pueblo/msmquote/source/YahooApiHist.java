package uk.co.pueblo.msmquote.source;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;

import uk.co.pueblo.msmquote.source.QuoteSummary.SummaryType;

public class YahooApiHist extends YahooQuote {

	// Instance variables
	private JsonNode resultJn;
	private String symbol;
	private int quoteDivisor;
	private int quoteIndex;
	private String quoteType;

	/**
	 * Constructor. 
	 * 
	 * @param apiUrl
	 * @throws IOException
	 */
	public YahooApiHist(String apiUrl) throws IOException {

		// Get quote data
		resultJn = YahooUtil.getJson(apiUrl).at("/chart/result/0");

		// Get symbol and quote divisor
		quoteDivisor = 1;
		symbol = resultJn.at("/meta").get("symbol").asText();
		quoteType = resultJn.at("/meta").get("instrumentType").asText();
		String quoteDivisorProp = baseProps.getProperty("divisor." + resultJn.at("/meta").get("currency").asText() + "." + quoteType);
		if (quoteDivisorProp != null) {
			quoteDivisor = Integer.parseInt(quoteDivisorProp);
		}

		isQuery = false;
		quoteIndex = 0;
		quoteSummary = new QuoteSummary();
	}

	@Override
	public Map<String, Object> getNext() throws IOException {
		Map<String, Object> quoteRow = new HashMap<>();

		if (!resultJn.at("/timestamp").has(quoteIndex)) {
			quoteSummary.log(LOGGER);
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
		quoteRow.put("dOpen", resultJn.at("/indicators/quote/0/open").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("dHigh", resultJn.at("/indicators/quote/0/high").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("dLow", resultJn.at("/indicators/quote/0/low").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("dPrice", resultJn.at("/indicators/quote/0/close").get(quoteIndex).asDouble() / quoteDivisor);
		quoteRow.put("vol", resultJn.at("/indicators/quote/0/volume").get(quoteIndex).asLong());

		quoteIndex++;
		quoteSummary.inc(quoteType, SummaryType.PROCESSED);
		return quoteRow;
	}

	@Override
	public boolean isQuery() {
		return isQuery;
	}
}