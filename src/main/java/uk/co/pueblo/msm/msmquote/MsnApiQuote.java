package uk.co.pueblo.msm.msmquote;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;

/**
 * An MSN API quote source.
 */
public class MsnApiQuote extends MsnApiSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsnApiQuote.class);
	private static final String URL_PARAM_IDS = PROPS.getProperty("url.param.ids");

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, String> symbolMap = new HashMap<>();

	/**
	 * Constructs an MSN API quote source using an auto-generated URL.
	 * 
	 * @param baseUrl    the base URL for the Yahoo Finance API
	 * @param secSymbols the list of Money security symbols for which to get quotes
	 * @param crncPairs  the list of Money currency pairs for which to get quotes
	 * @throws QuoteSourceException
	 */
	public MsnApiQuote(String baseUrl, List<String[]> secSymbols, List<String[]> crncPairs) throws QuoteSourceException {

		// Build MSN instrument IDs string
		StringJoiner msnIdsSj = new StringJoiner(",");
		int msnIdCount = 0;
		for (String secSymbol[] : secSymbols) {
			if (secSymbol.length == 3) {
				Matcher altSymbolMatcher = PATTERN_ALT_SYMBOL.matcher(secSymbol[2]);
				if (altSymbolMatcher.find()) {
					String msnId = secSymbol[2].substring(altSymbolMatcher.start() + 10, altSymbolMatcher.start() + 16);
					msnIdsSj.add(msnId);
					symbolMap.put(msnId, secSymbol[0]);
					msnIdCount++;
				}
			}
		}
		String msnIds = msnIdsSj.toString();
		if (msnIdCount == 0) {
			LOGGER.warn("No instrument IDs found to update in Money file");
		} else {
			LOGGER.info("Building URL with {} instrument ID{}: {}", msnIdCount, msnIdCount == 1 ? "" : "s", msnIds);
		}
		
		// Print currency pair symbols to log
		StringJoiner cpSymbolsSj = new StringJoiner(",");
		int cpCount = 0;
		for (String[] crncPair : crncPairs) {
			cpSymbolsSj.add(crncPair[0]);
			cpCount++;
		}
		if (cpCount == 0) {
			LOGGER.warn("No currency pairs found to update in Money file");
		} else {
			LOGGER.info("Obtaining exchange rate data for {} currency pair{}: {}", cpCount, cpCount == 1 ? "" : "s", cpSymbolsSj.toString());
		}

		// Get quote data from MSN APIs
		JsonNode jn = null;
		if (!baseUrl.endsWith(URL_PARAM_IDS + "?") && msnIdCount + cpCount > 0) {

			// Get security quote data
			if (msnIdCount > 0) {
				List<String> apiUrls = new ArrayList<>();
				if (baseUrl.endsWith(URL_PARAM_IDS)) {
					apiUrls.add(baseUrl + msnIds);
				} else {
					int urlIndex = 1;
					while ((baseUrl = PROPS.getProperty("url.api." + urlIndex++)) != null) {
						apiUrls.add(baseUrl + URL_PARAM_IDS + msnIds);
					}
				}
				jn = getQuoteData(apiUrls);
			}

			// Get exchange rate quote data
			List<String> fxApiUrls = Arrays.asList(PROPS.getProperty("url.api.fx"));
			JsonNode fxJn = getQuoteData(fxApiUrls);

			// Add exchange rate objects to tree model
			ArrayNode an = (ArrayNode) jn;
			for (String[] crncPair : crncPairs) {
				double[] rate = { 0, 0 };
				for (int i = 0; i < 2; i++) {
					int index = 0;
					JsonNode rateJn;
					while ((rateJn = fxJn.get(index++)) != null) {
						if (rateJn.get("An").asText().equals(crncPair[0].substring(i * 3, i * 3 + 3)))
							rate[i] = rateJn.get("Rt").asDouble();
					}
				}
				ArrayNode cpAn = MAPPER.createArrayNode();
				try {
					cpAn = (ArrayNode) MAPPER.readTree("[[ { \"price\": " + rate[1] / rate[0] + ", \"currency\": \"\", \"securityType\": \"currencyPair\", \"instrumentId\": \"" + crncPair[0] + "\" } ]]");
				} catch (JsonProcessingException e) {
					LOGGER.error("Invalid exchange rate data: {}", e.getMessage());
					LOGGER.debug("Exception occured!", e);
				}
				an.addAll(cpAn);
				symbolMap.put(crncPair[0], crncPair[0]);
			}
			resultIt = jn.values();
			return;
		}
	}

	/**
	 * Constructs an MSN API quote source using an auto-generated URL.
	 * 
	 * @param secSymbols the list of Money security symbols for which to get quotes
	 * @param crncPairs  the list of Money currency pairs for which to get quotes
	 * @throws QuoteSourceException
	 */
	public MsnApiQuote(List<String[]> secSymbols, List<String[]> crncPairs) throws QuoteSourceException {
		this("", secSymbols, crncPairs);
	}

	/**
	 * Constructs an MSN API quote source from a user-supplied URL.
	 * 
	 * @param apiUrl the complete URL for the MSN API
	 * @throws QuoteSourceException
	 */
	public MsnApiQuote(String apiUrl) throws QuoteSourceException {

		// Get quote data from api
		List<String> apiUrls = Arrays.asList(apiUrl);
		JsonNode jn = getQuoteData(apiUrls);
		resultIt = jn.elements();
		return;
	}

	public Map<String, Object> getNext() {

		// Get next JSON object node from iterator
		Map<String, Object> returnRow = new HashMap<>();
		if (resultIt == null || !resultIt.hasNext())
			return returnRow;
		JsonNode result = resultIt.next();
		if (result.isArray())
			result = result.get(0);

		try {
			// Add security type to return row
			String securityType = result.get("securityType").asText();
			returnRow.put("xType", PROPS.getProperty("security-type." + securityType));

			// Add symbol to return row
			if (symbolMap.isEmpty()) {
				returnRow.put("xSymbol", result.get("symbol").asText());
			} else {
				returnRow.put("xSymbol", symbolMap.get(result.get("instrumentId").asText()));
			}

			// Get quote adjuster for currency and quote type
			int quoteAdjuster = getAdjuster(PROPS, result.get("currency").asText(), securityType);

			// Add quote values to return row
			String prop;
			int n = 1;
			while ((prop = PROPS.getProperty("api." + securityType + '.' + n++)) != null) {
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