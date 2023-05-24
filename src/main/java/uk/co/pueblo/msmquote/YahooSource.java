package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.io.InputStream;
import java.net.CookieManager;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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
	private static final String COOKIE_URL = "https://fc.yahoo.com";
	private static final String CRUMB_URL = "https://query2.finance.yahoo.com/v1/test/getcrumb";

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
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	static JsonNode getJson(String param) throws APIException, URISyntaxException, IOException, InterruptedException {

		// Get http client timeout from properties file
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("httpclient.timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);

		// Get Yahoo cookie and crumb
		HttpClient httpClient = HttpClient.newBuilder().cookieHandler(new CookieManager()).connectTimeout(Duration.ofSeconds(httpClientTimeout)).build();
		HttpRequest request;
		HttpResponse<String> response;
		
		request = buildHttpRequest(COOKIE_URL);
		httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		request = buildHttpRequest(CRUMB_URL);
		String crumb = "&crumb=" + httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();

		// Now get data from the API
		boolean loop = true;
		int n = 0;
		String prop;
		String apiUrl;
		while (loop) {
			if (param.startsWith("https://")) {
				apiUrl = param;
				loop = false;
			} else {
				if ((prop = PROPS.getProperty("api.url." + ++n)) == null) {
					break;
				} else {
					apiUrl = prop + param;
				}
			}

			try {
				request = buildHttpRequest(apiUrl + crumb);
				LOGGER.info("Requesting quote data from Yahoo Finance API, url={}", n);
				response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
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
	
	static HttpRequest buildHttpRequest(String inUrl) throws URISyntaxException, MalformedURLException {
		URL url = new URL(inUrl);
		URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
		LOGGER.debug(uri.toASCIIString());
		return HttpRequest.newBuilder(uri).GET().build();
	}

}