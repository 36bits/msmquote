package uk.co.pueblo.msmquote;

import java.net.CookieManager;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

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

	// Class variables
	private static HttpClient httpClient;
	private static String crumb = "";

	static {
		// Set up http client
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("httpclient.timeout"));		
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);
		CookieManager cm = new CookieManager();
		httpClient = HttpClient.newBuilder().cookieHandler(cm).connectTimeout(Duration.ofSeconds(httpClientTimeout)).build();

		// Get Yahoo cookie and crumb
		try {
			int n = 0;
			String cookieUrl;			
			while (true) {
				if ((cookieUrl = PROPS.getProperty("cookie.url." + ++n)) == null) {
					break;
				}
				LOGGER.info("Getting API crumb, cookie url={}", n);
				httpClient.send(HttpRequest.newBuilder(new URI(cookieUrl)).GET().build(), HttpResponse.BodyHandlers.ofString());
				if (!cm.getCookieStore().getCookies().isEmpty()) {
					break;
				}
			}
			crumb = httpClient.send(HttpRequest.newBuilder(new URI(PROPS.getProperty("crumb.url"))).setHeader("User-Agent", HTTP_REQ_UA).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
		} catch (Exception e) {
			setStatus(SourceStatus.FATAL);
			LOGGER.debug("Exception occurred!", e);
			LOGGER.fatal(e);
		}

		// Validate crumb
		if (crumb.isEmpty() || crumb.contains(" ")) {
			setStatus(SourceStatus.FATAL);
			LOGGER.fatal("Received invalid API crumb, crumb={}", crumb.trim());
			throw new RuntimeException();
		} else {
			LOGGER.info("API crumb={}", crumb);
			crumb = "&crumb=" + URLEncoder.encode(crumb, StandardCharsets.UTF_8); // create crumb parameter
		}
	}

	static JsonNode getJson(String param) throws APIException {

		// Get data from the API
		boolean loop = true;
		int n = 0;
		String apiUrl;
		HttpResponse<String> response;
		while (loop) {
			if (param.startsWith("https://")) {
				apiUrl = param;
				loop = false;
			} else {
				if ((apiUrl = PROPS.getProperty("api.url." + ++n)) == null) {
					break;
				} else {
					apiUrl = apiUrl + "symbols=" + URLEncoder.encode(param, StandardCharsets.UTF_8);
				}
			}

			apiUrl = apiUrl + crumb; // add crumb parameter to url
			LOGGER.debug("URL={}", apiUrl);

			try {
				URI uri = new URI(apiUrl);
				LOGGER.info("Requesting quote data from Yahoo Finance API, url={}", n);
				response = httpClient.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofString());
				LOGGER.info("Received {} bytes from Yahoo Finance API", response.body().length());

				// ObjectMapper mapper = new ObjectMapper();
				// ObjectMapper mapper = JsonMapper.builder().enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN).build();
				ObjectMapper mapper = JsonMapper.builder().build();
				JsonNode responseJn = mapper.readTree(response.body());

				if (responseJn.at("/finance/error").has("code")) {
					throw new APIException("Yahoo Finance API error response: " + responseJn.at("/finance/error").get("code").asText() + ", " + responseJn.at("/finance/error").get("description").asText());
				}
				return responseJn;
			} catch (Exception e) {
				LOGGER.error(e);
				setStatus(SourceStatus.ERROR);
			}
		}
		throw new APIException("All Yahoo Finance API requests failed!");
	}
}