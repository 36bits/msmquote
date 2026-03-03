package uk.co.pueblo.msm.msmquote;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

import javax.net.ssl.SSLParameters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * Parent class for MSN Money API quote sources.
 */
abstract class MsnApiSource extends MsnSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsnApiSource.class);
	private static final HttpClient HTTP_CLIENT;
	private static final String HTTP_USER_AGENT = PROPS.getProperty("http.user-agent");
	static final ObjectMapper MAPPER = JsonMapper.builder().build();
	static final Pattern PATTERN_ALT_SYMBOL = Pattern.compile("AltSymbol=[a-z0-9#]{6}");

	static {
		// Set up http client
		String[] tlsVersions = PROPS.getProperty("http.tls-versions").split("\\s");
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("http.client-timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);
		SSLParameters sslParams = new SSLParameters();
		sslParams.setProtocols(tlsVersions);
		HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(httpClientTimeout)).sslParameters(sslParams).build();
	}

	MsnApiSource() {
	}

	static JsonNode getQuoteData(List<String> apiUrls) throws QuoteSourceException {

		final String regexQuoteData = PROPS.getProperty("regex.quote-data");
		LOGGER.debug("Regex: quote data={}", regexQuoteData);

		int apiUrlIdx = 1;

		for (String apiUrl : apiUrls) {
			LOGGER.debug("URL={}", apiUrl);
			LOGGER.info("Trying MSN Money API URL #{}", apiUrlIdx++);
			try {
				String apiResponse = HTTP_CLIENT.send(HttpRequest.newBuilder(new URI(apiUrl)).setHeader("User-Agent", HTTP_USER_AGENT).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
				LOGGER.info("Received {} bytes from MSN Money API", apiResponse.length());
				if (apiResponse.matches(regexQuoteData)) {
					// Return quote data as Jackson tree model
					JsonNode jn = MAPPER.readTree(apiResponse);
					return jn;
				} else {
					LOGGER.warn("Failed to get valid quote data, API response={}", apiResponse);
					sourceClassStatus = SourceStatus.WARN;
				}
			} catch (Exception e) {
				LOGGER.debug("Exception occurred!", e);
				LOGGER.warn("Failed to get valid quote data: {}", e.toString());
				sourceClassStatus = SourceStatus.WARN;
			}
		}
		throw new QuoteSourceException("All MSN Money API requests failed!");
	}
}