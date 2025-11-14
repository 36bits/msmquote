package uk.co.pueblo.msm.msmquote;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * An MSN API quote source.
 */
public class MsnApiQuote extends MsnApiSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsnApiQuote.class);	

	// Instance variables
	private Iterator<JsonNode> resultIt;
	private Map<String, String> symbolMap = new HashMap<>();

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

	public Map<String, String> getNext() {
		// Get next JSON node from iterator
		Map<String, String> returnRow = new HashMap<>();
		if (resultIt == null || !resultIt.hasNext()) {
			return returnRow;
		}
		JsonNode result = resultIt.next();
				
		// Iterate to first non-array node in result
		while (result.isArray()) {
			result = result.get(0);
		}

		try {
			// Add security type to return row
			String securityType = result.get("securityType").asText();
			returnRow.put("xType", PROPS.getProperty("security-type." + securityType));

			// Add symbol to return row
			String msnSymbol = result.get("symbol").asText();
			if (symbolMap.isEmpty()) {
				returnRow.put("xSymbol", msnSymbol);
			} else {
				returnRow.put("xSymbol", symbolMap.get(msnSymbol));
			}

			// Get quote adjuster for currency and quote type
			int quoteAdjuster = getAdjuster(PROPS, result.get("currency").asText(), securityType);

			// Add quote values to return row
			String prop;
			int n = 1;
			while ((prop = PROPS.getProperty("api." + securityType + '.' + n++)) != null) {
				String[] columnMap = prop.split(",");
				if (result.has(columnMap[0])) {					
					String value = result.get(columnMap[0]).asText();
					if (columnMap.length == 3) value = adjustQuote(value, columnMap[2], quoteAdjuster);
					returnRow.put(columnMap[1], value);
				}
			}

		} catch (NullPointerException e) {
			LOGGER.debug("Exception occurred!", e);
		}

		return returnRow;
	}
}