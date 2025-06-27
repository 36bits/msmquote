package uk.co.pueblo.msm.msmquote;

import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.StringJoiner;
import java.util.prefs.Preferences;

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
	private static final HttpClient HTTP_CLIENT;
	private static final CookieManager COOKIE_MGR = new CookieManager();
	private static final String HTTP_USER_AGENT = PROPS.getProperty("http.user-agent");
	private static final String COOKIE_NAME = PROPS.getProperty("http.cookie-name");
	private static final Preferences PREFS_NODE = Preferences.userNodeForPackage(QuoteSource.class);
	private static final String PREF_KEY_COOKIE = PROPS.getProperty("pref-key.cookie");
	private static final String PREF_KEY_CRUMB = PROPS.getProperty("pref-key.crumb");

	// Class variables
	private static String crumb = null;

	static {
		// Get cached Yahoo cookie and API crumb from preferences
		try {
			List<HttpCookie> cookies = HttpCookie.parse(PREFS_NODE.get(PREF_KEY_COOKIE, null));
			HttpCookie yahooCookie = cookies.get(0);
			COOKIE_MGR.getCookieStore().add(null, yahooCookie);
			crumb = PREFS_NODE.get(PREF_KEY_CRUMB, null);
			LOGGER.info("Using cached Yahoo cookie and API crumb");
		} catch (Exception e) {
			LOGGER.debug("Exception occurred!", e);
			LOGGER.warn("Failed to get cached Yahoo cookie and API crumb");
			sourceClassStatus = SourceStatus.WARN;
		}

		// Set up http client
		String[] tlsVersions = PROPS.getProperty("http.tls-versions").split("\\s");
		int httpClientTimeout = Integer.parseInt(PROPS.getProperty("http.client-timeout"));
		LOGGER.info("HTTP client timeout={}s", httpClientTimeout);
		SSLParameters sslParams = new SSLParameters();
		sslParams.setProtocols(tlsVersions);
		HTTP_CLIENT = HttpClient.newBuilder().cookieHandler(COOKIE_MGR).connectTimeout(Duration.ofSeconds(httpClientTimeout)).sslParameters(sslParams).build();
	}

	YahooApiSource() {
	}

	static JsonNode getQuoteData(List<String> apiUrls) throws QuoteSourceException {
		
		HttpCookie yahooCookie = null;
		int apiUrlIdx = 1;
		
		for (String apiUrl : apiUrls) {
			LOGGER.debug("URL={}", apiUrl);
			LOGGER.info("Trying Yahoo Finance API URL #{}", apiUrlIdx++);
			int cookieUrlIdx = 1;
			while (true) {
				try {
					String apiResponse = HTTP_CLIENT.send(HttpRequest.newBuilder(new URI(apiUrl + "&crumb=" + crumb)).setHeader("User-Agent", HTTP_USER_AGENT).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
					LOGGER.info("Received {} bytes from Yahoo Finance API", apiResponse.length());
					if (apiResponse.matches("^\\{\"(quoteResponse|chart)\":\\{\"result\":\\[.*\\Q],\"error\":null}}\\E$")) {
						// Write new cookie and crumb to preferences
						if (yahooCookie != null) {
							// Build Set-Cookie header
							String expires = OffsetDateTime.now(ZoneOffset.UTC).plus(Duration.ofSeconds(yahooCookie.getMaxAge())).format(DateTimeFormatter.RFC_1123_DATE_TIME);
							StringJoiner yahooCookieHdr = new StringJoiner("; ");
							yahooCookieHdr.add(yahooCookie.toString());
							yahooCookieHdr.add("Expires=" + expires);
							yahooCookieHdr.add("Domain=" + yahooCookie.getDomain());
							yahooCookieHdr.add("Path=" + yahooCookie.getPath());
							// cacheCookie.add("SameSite=None");
							if (yahooCookie.getSecure())
								yahooCookieHdr.add("Secure");
							if (yahooCookie.isHttpOnly())
								yahooCookieHdr.add("HttpOnly");
							PREFS_NODE.put(PREF_KEY_COOKIE, yahooCookieHdr.toString());
							PREFS_NODE.put(PREF_KEY_CRUMB, crumb);
						}
						// Return quote data as JSON node
						ObjectMapper mapper = JsonMapper.builder().build();
						JsonNode jn = mapper.readTree(apiResponse);
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

				// Get new Yahoo cookie and API crumb
				crumb = null;
				String cookieUrl;				
				while ((cookieUrl = PROPS.getProperty("url.cookie." + cookieUrlIdx)) != null) {
					// First get cookie
					try {
						LOGGER.info("Getting cookie from cookie URL #{}: {}", cookieUrlIdx++, cookieUrl);
						HTTP_CLIENT.send(HttpRequest.newBuilder(new URI(cookieUrl)).setHeader("User-Agent", HTTP_USER_AGENT).GET().build(), HttpResponse.BodyHandlers.ofString());
						List<HttpCookie> cookies = COOKIE_MGR.getCookieStore().getCookies();
						for (HttpCookie cookie : cookies) {
							if (cookie.getName().equals(COOKIE_NAME)) {
								yahooCookie = cookie;
								break;
							}
						}
					} catch (Exception e) {
						LOGGER.debug("Exception occurred!", e);
						LOGGER.warn("Failed to get cookie: {}", e.toString());
						sourceClassStatus = SourceStatus.WARN;
						continue;
					}
					// Then get crumb
					if (yahooCookie != null) {
						try {
							crumb = HTTP_CLIENT.send(HttpRequest.newBuilder(new URI(PROPS.getProperty("url.crumb"))).setHeader("User-Agent", HTTP_USER_AGENT).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
							if (crumb.matches("^\\S{11}$")) {
								LOGGER.info("Got API crumb, value={}", crumb);
								break;
							} else {
								LOGGER.warn("Failed to get valid API crumb, response={}", crumb.trim());
								sourceClassStatus = SourceStatus.WARN;
								crumb = null;
							}
						} catch (Exception e) {
							LOGGER.debug("Exception occurred!", e);
							LOGGER.warn("Failed to get crumb: {}", e.toString());
							sourceClassStatus = SourceStatus.WARN;
						}
					} else {
						LOGGER.warn("Failed to get cookie");
						sourceClassStatus = SourceStatus.WARN;
					}
				}
				// If no crumb then try next API URL
				if (crumb == null)
					break;
			}			
		}
		throw new QuoteSourceException("All Yahoo Finance API requests failed!");
	}
}