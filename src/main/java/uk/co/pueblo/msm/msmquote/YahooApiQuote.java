package uk.co.pueblo.msm.msmquote;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

/**
 * A Yahoo Finance API quote source.
 */
public class YahooApiQuote extends YahooApiSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiQuote.class);
	private static final String URL_PARAM_SYMBOLS = PROPS.getProperty("url.param.symbols");
	private static final String JSON_ROOT = "/quoteResponse/result";

	// Instance variables
	private Iterator<JsonNode> resultIt;
	Map<String, String> symbolMap = new HashMap<>();

	/**
	 * Constructs a Yahoo Finance API quote source using an auto-generated URL.
	 * 
	 * @param baseUrl    the base URL for the Yahoo Finance API
	 * @param secSymbols the list of Money security symbols for which to get quotes
	 * @param crncPairs  the list of Money currency pairs for which to get quotes
	 * @throws QuoteSourceException
	 */
	public YahooApiQuote(String baseUrl, List<String[]> secSymbols, List<String[]> crncPairs) throws QuoteSourceException {

		String yahooSymbol = "";

		// Build security symbol map and Yahoo Finance symbols string
		Map<String, String> secSymbolMap = new HashMap<>();
		StringJoiner secSymbolsSj = new StringJoiner(",");
		for (String secSymbol[] : secSymbols) {
			if ((yahooSymbol = getYahooSymbol(secSymbol[0], secSymbol[1])) == null) {
				LOGGER.warn("Cannot find Yahoo Finance exchange suffix for symbol {}, country code={}", secSymbol[0], secSymbol[1]);
				sourceStatus = SourceStatus.WARN;
				yahooSymbol = secSymbol[0];
			}
			// Update the symbol translation map and add the Yahoo symbol to the security
			// symbols string joiner
			if (updateSymbolMap(yahooSymbol, secSymbol[0], secSymbolMap))
				secSymbolsSj.add(yahooSymbol);
		}
		int secCount = secSymbolMap.size();
		if (secCount == 0) {
			LOGGER.warn("No security symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with {} security symbol{}: {}", secCount, secCount == 1 ? "" : "s", secSymbolsSj.toString());
			symbolMap.putAll(secSymbolMap);
		}

		// Build currency pair symbol map and Yahoo Finance symbols string
		Map<String, String> cpSymbolMap = new HashMap<>();
		StringJoiner cpSymbolsSj = new StringJoiner(",");
		for (String crncPair[] : crncPairs) {
			yahooSymbol = getYahooSymbol(crncPair[0], crncPair[1]);
			// Update the symbol translation map and add the Yahoo symbol to the currency
			// pair symbols string joiner
			if (updateSymbolMap(yahooSymbol, crncPair[0], cpSymbolMap))
				cpSymbolsSj.add(yahooSymbol);
		}
		int cpCount = cpSymbolMap.size();
		if (cpCount == 0) {
			LOGGER.warn("No FX symbols found to update in Money file");
		} else {
			LOGGER.info("Building URL with {} currency pair{}: {}", cpCount, secCount == 1 ? "" : "s", cpSymbolsSj.toString());
			symbolMap.putAll(cpSymbolMap);
		}

		// Get quote data from api
		String allSymbols = URLEncoder.encode(secSymbolsSj.merge(cpSymbolsSj).toString(), StandardCharsets.UTF_8);
		if (!baseUrl.endsWith(URL_PARAM_SYMBOLS + "?") && secCount + cpCount > 0) {
			List<String> apiUrls = new ArrayList<>();
			if (baseUrl.endsWith(URL_PARAM_SYMBOLS))
				apiUrls.add(baseUrl + allSymbols);
			else {
				int urlIndex = 1;
				while ((baseUrl = PROPS.getProperty("url.api." + urlIndex++)) != null) {
					apiUrls.add(baseUrl + URL_PARAM_SYMBOLS + allSymbols);
				}
			}
			JsonNode jn = getQuoteData(apiUrls);
			resultIt = jn.at(JSON_ROOT).elements();
			return;
		}
	}

	/**
	 * Constructs a Yahoo Finance API quote source using an auto-generated URL.
	 * 
	 * @param baseUrl    the base URL for the Yahoo Finance API
	 * @param secSymbols the list of Money security symbols for which to get quotes
	 * @param crncPairs  the list of Money currency pairs for which to get quotes
	 * @throws QuoteSourceException
	 */
	public YahooApiQuote(List<String[]> secSymbols, List<String[]> crncPairs) throws QuoteSourceException {
		this("", secSymbols, crncPairs);
	}

	/**
	 * Constructs a Yahoo Finance API quote source from a user-supplied URL.
	 * 
	 * @param apiUrl the complete URL for the Yahoo Finance API
	 * @throws QuoteSourceException
	 */
	public YahooApiQuote(String apiUrl) throws QuoteSourceException {

		// Encode symbols
		if (apiUrl.contains(URL_PARAM_SYMBOLS)) {
			String[] apiUrlParts = apiUrl.split(URL_PARAM_SYMBOLS, 2);
			apiUrl = apiUrlParts[0] + URL_PARAM_SYMBOLS + URLEncoder.encode(apiUrlParts[1], StandardCharsets.UTF_8);
		}

		// Get quote data from api
		List<String> apiUrls = Arrays.asList(apiUrl);
		JsonNode jn = getQuoteData(apiUrls);
		resultIt = jn.at(JSON_ROOT).elements();
		return;
	}

	public Map<String, Object> getNext() {
		// Get next JSON node from iterator
		Map<String, Object> returnRow = new HashMap<>();
		if (resultIt == null || !resultIt.hasNext()) {
			return returnRow;
		}
		JsonNode result = resultIt.next();

		try {
			// Add quote type to return row
			String quoteType = result.get("quoteType").asText();
			returnRow.put("xType", quoteType);

			// Add SEC symbols list to return row
			String yahooSymbol = result.get("symbol").asText();
			if (symbolMap.isEmpty()) {
				returnRow.put("xSymbol", yahooSymbol);
			} else {
				returnRow.put("xSymbol", symbolMap.get(yahooSymbol));
			}

			// Get quote adjuster for currency and quote type
			int quoteAdjuster = getAdjuster(PROPS, result.get("currency").asText(), quoteType);

			// Add quote values to return row
			String prop;
			int n = 1;
			while ((prop = PROPS.getProperty("api." + quoteType + '.' + n++)) != null) {
				String[] columnMap = prop.split(",");
				if (result.has(columnMap[0])) {
					JsonNode valueJn = result.get(columnMap[0]);
					if (valueJn.getNodeType().equals(JsonNodeType.NUMBER)) {
						double value = valueJn.asDouble();
						if (columnMap.length == 3)
							value = adjustQuote(value, columnMap[2], quoteAdjuster);
						returnRow.put(columnMap[1], value);
					} else if (valueJn.getNodeType().equals(JsonNodeType.STRING)) {
						returnRow.put(columnMap[1], valueJn.asText());
					}
				}
			}

		} catch (NullPointerException e) {
			LOGGER.debug("Exception occurred!", e);
		}

		return returnRow;
	}
}