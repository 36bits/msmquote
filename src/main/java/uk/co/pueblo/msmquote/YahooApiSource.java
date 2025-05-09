package uk.co.pueblo.msmquote;

import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import javax.net.ssl.SSLParameters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Parent class for the Yahoo Finance API quote sources.
 */
abstract class YahooApiSource extends YahooSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooApiSource.class);
	private static final String HTTP_REQ_UA = PROPS.getProperty("httprequest.useragent");
	private static final String COOKIE_NAME = "A3";
	private static final String[] TLS_VERSIONS = { "TLSv1.3" };
	static final String SYMBOLS_PARAM = "symbols=";

	// Class variables
	private static HttpClient httpClient;
	private static String crumb = null;

	static {
		// Set up http client
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("httpclient.timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);
		CookieManager cm = new CookieManager();
		SSLParameters sp = new SSLParameters();
		sp.setProtocols(TLS_VERSIONS);
		httpClient = HttpClient.newBuilder().cookieHandler(cm).connectTimeout(Duration.ofSeconds(httpClientTimeout)).sslParameters(sp).build();

		// Get Yahoo cookie and crumb
		int n = 0;
		String cookieUrl;
		while (crumb == null) {
			if ((cookieUrl = PROPS.getProperty("cookie.url." + ++n)) == null) {
				break;
			}
			try {
				LOGGER.info("Getting cookie from {}", cookieUrl);
				httpClient.send(HttpRequest.newBuilder(new URI(cookieUrl)).GET().build(), HttpResponse.BodyHandlers.ofString());
				List<HttpCookie> cookies = cm.getCookieStore().getCookies();
				for (HttpCookie cookie : cookies) {
					if (cookie.getName().equals(COOKIE_NAME)) {
						crumb = httpClient.send(HttpRequest.newBuilder(new URI(PROPS.getProperty("crumb.url"))).setHeader("User-Agent", HTTP_REQ_UA).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
						// Validate crumb
						if (crumb.matches("^\\S{11}$")) {
							LOGGER.info("API crumb={}", crumb);
							crumb = "&crumb=" + crumb; // create crumb parameter
						} else {
							LOGGER.warn("Received invalid API crumb, data={}", crumb.trim());
							sourceClassStatus = SourceStatus.WARN;
							crumb = null;
						}
						break;
					}
					LOGGER.warn("Failed to get cookie");
					sourceClassStatus = SourceStatus.WARN;
				}
			} catch (Exception e) {
				LOGGER.debug("Exception occurred!", e);
				LOGGER.warn(e);
			}
		}
	}

	YahooApiSource() throws QuoteSourceException {
		// Check for existence of crumb
		if (crumb == null) {
			throw new QuoteSourceException("No Yahoo Finance API crumb!");
		}
	}

	static JsonNode getJson(String apiUrl) throws QuoteSourceException {
		// Get data from the API
		String apiResponse = null;
		JsonNode jn = null;
		apiUrl = apiUrl + crumb; // add crumb parameter to url
		LOGGER.debug("URL={}", apiUrl);

		try {
			LOGGER.info("Requesting quote data from Yahoo Finance API");			
			apiResponse = httpClient.send(HttpRequest.newBuilder(new URI(apiUrl)).setHeader("User-Agent", HTTP_REQ_UA).GET().build(), HttpResponse.BodyHandlers.ofString()).body();		
			LOGGER.info("Received {} bytes from Yahoo Finance API", apiResponse.length());
			// ObjectMapper mapper = new ObjectMapper();
			// ObjectMapper mapper =
			// JsonMapper.builder().enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN).build();			
			ObjectMapper mapper = JsonMapper.builder().build();
			jn = mapper.readTree(apiResponse);
		} catch (Exception e) {
			LOGGER.debug("Exception occurred!", e);
			LOGGER.error(e);
			LOGGER.error("Received data from Yahoo Finance API={}", apiResponse);
		}

		// General validation of received JSON data
		if (jn.isEmpty()) {
			throw new QuoteSourceException("Received empty JSON response from Yahoo Finance API");			
		} else if (jn.at("/finance/error").has("code")) {
			throw new QuoteSourceException("Received JSON error response from Yahoo Finance API: " + jn);
		}
		return jn;
	}
}