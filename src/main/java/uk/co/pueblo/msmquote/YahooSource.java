package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

abstract class YahooSource implements QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooSource.class);
	
	// Class variables
	static int finalStatus = SOURCE_OK;

	YahooSource(String propsFile) {
		// Open properties
		if (!propsFile.isEmpty()) {
			try {
				InputStream propsIs = getClass().getClassLoader().getResourceAsStream(propsFile);
				PROPS.load(propsIs);
			} catch (IOException e) {
				LOGGER.fatal(e);
			}
		}
	}

	/**
	 * Gets JSON quote data from a web API.
	 * 
	 * @param param either the URL of the web API or a comma-separated list of symbols
	 * @return the quote data
	 * @throws APIException
	 */
	static JsonNode getJson(String param) throws APIException {

		// Get api http timeout from properties file
		int apiTimeout = Integer.parseInt(PROPS.getProperty("api.timeout"));
		LOGGER.info("Yahoo Finance API request timeout={}s", apiTimeout);

		// Build and send http requests to API
		boolean loop = true;
		int n = 0;
		String prop;
		String url;
		while (loop) {
			if (param.startsWith("https://")) {
				url = param;
				loop = false;
			} else {
				if ((prop = PROPS.getProperty("api.url." + ++n)) == null) {
					break;
				} else {
					url = prop + param;
				}
			}
			try {
				URL apiUrl = new URL(url);
				URI apiUri = new URI(apiUrl.getProtocol(), apiUrl.getUserInfo(), apiUrl.getHost(), apiUrl.getPort(), apiUrl.getPath(), apiUrl.getQuery(), apiUrl.getRef());
				LOGGER.debug(apiUri.toASCIIString());
				HttpClient httpClient = HttpClient.newHttpClient();
				HttpRequest request = HttpRequest.newBuilder(apiUri).timeout(Duration.ofSeconds(apiTimeout)).GET().build();

				LOGGER.info("Requesting quote data from Yahoo Finance API, url={}", n);
				HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
				LOGGER.info("Received {} bytes from Yahoo Finance API", response.body().length());

				ObjectMapper mapper = new ObjectMapper();
				JsonNode responseJn = mapper.readTree(response.body());
				if (responseJn.at("/finance/error").has("code")) {
					throw new APIException("Yahoo Finance API error response: " + responseJn.at("/finance/error").get("code").asText() + ", " + responseJn.at("/finance/error").get("description").asText());
				}
				return responseJn;
			} catch (Exception e) {
				LOGGER.error(e);
				setStatus(SOURCE_ERROR);
			}
		}
		throw new APIException("All Yahoo Finance API requests failed!");
	}

	/**
	 * Generates a Yahoo symbol from the Money symbol.
	 * 
	 * @param symbol  the Money symbol for the security
	 * @param country the Money country for the security
	 * @param props   the YahooQuote properties
	 * @return the equivalent Yahoo symbol
	 */
	static String getYahooSymbol(String symbol, String country, Properties props) {
		String yahooSymbol = symbol;
		String prop;
		if (symbol.matches("^\\$US:.*")) {
			// Symbol is in Money index format '$US:symbol'
			if ((prop = props.getProperty("index." + symbol.substring(4))) != null) {
				yahooSymbol = prop;
			}
		} else if (symbol.matches("^\\$..:.*")) {
			// Symbol is in Money index format '$xx:symbol'
			yahooSymbol = "^" + symbol.substring(4);
		} else if (symbol.matches("^\\$.*")) {
			// Symbol is in Money index format '$symbol'
			yahooSymbol = "^" + symbol.substring(1);
		} else if (symbol.matches("^..:.*")) {
			// Symbol is in Money security format 'xx:symbol'
			if ((prop = props.getProperty("exchange." + country)) != null) {
				yahooSymbol = symbol.substring(3) + prop;
			}
		} else if (!symbol.matches("(.*\\..$|.*\\...$|^\\^.*)")) {
			// Symbol is not already in Yahoo format 'symbol.x', 'symbol.xx' or '^symbol"
			if ((prop = props.getProperty("exchange." + country)) != null) {
				yahooSymbol = symbol + prop;
			}
		}
		return yahooSymbol.toUpperCase();
	}

	static int getDivisor(String quoteCurrency, String quoteType) {
		String prop;
		return ((prop = PROPS.getProperty("divisor." + quoteCurrency + "." + quoteType)) == null) ? 1 : Integer.parseInt(prop);
	}

	static int getMultiplier(String quoteCurrency, String quoteType) {
		String prop;
		return ((prop = PROPS.getProperty("multiplier." + quoteCurrency + "." + quoteType)) == null) ? 100 : Integer.parseInt(prop);
	}

	static String adjustQuote(String value, String adjuster, int divisor, int multiplier) {
		if (adjuster.equals("d")) {
			value = String.valueOf(Double.parseDouble(value) / divisor);
		} else if (adjuster.equals("m")) {
			value = String.valueOf(Double.parseDouble(value) * multiplier);
		}
		return value;
	}
	
	public int getStatus() {
		return finalStatus;
	}
	
	static void setStatus(int status) {
		if (status > finalStatus) {
			finalStatus = status;
		}
		return;
	}
	
}