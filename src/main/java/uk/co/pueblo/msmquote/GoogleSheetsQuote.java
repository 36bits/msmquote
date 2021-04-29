package uk.co.pueblo.msmquote;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;

class GoogleSheetsQuote extends Quote {

	// Constants
	private static final String PROPS_FILE = "GoogleSheetsQuote.properties";
	private static final int HEADER_COLUMN = 0;
	private static final String HEADER_FLAG = "symbol";

	// Constants for Google API
	private static final String APPLICATION_NAME = "msmquote Google Sheets source";
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	private static final String TOKENS_DIRECTORY_PATH = "tokens";
	private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
	private static final String CREDENTIALS_FILE_PATH = "/credentials.json";

	// Instance variables
	final List<List<Object>> quoteRows;
	List<Object> headerRow;
	int quoteIndex = 0;

	// Constructor
	GoogleSheetsQuote(String spreadsheetId, String range) throws IOException, GeneralSecurityException {
		super(PROPS_FILE);
		// Build a new authorised API client service.
		final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
		Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT)).setApplicationName(APPLICATION_NAME).build();
		ValueRange response = service.spreadsheets().values().get(spreadsheetId, range).execute();
		quoteRows = response.getValues();
	}

	@Override
	Map<String, Object> getNext() throws IOException {
		Map<String, Object> returnRow = new HashMap<>();
		String googleSymbol;
		String quoteType;
		String prop;
		List<Object> quoteRow = null;

		// Get quote row
		while (quoteIndex < quoteRows.size()) {
			quoteRow = quoteRows.get(quoteIndex++);
			googleSymbol = (String) quoteRow.get(0);
			if (googleSymbol.equals(HEADER_FLAG)) {
				headerRow = quoteRow;
				continue;
			}
			// Build return row
			String columnName;
			double columnValue;
			for (int n = 1; n < quoteRow.size(); n++) {
				columnName = (String) headerRow.get(n);
				if (columnName.equals("type")) {
					quoteType = (String) quoteRow.get(n);
				} else if (columnName.equals("xSymbol")) {
					returnRow.put(columnName, quoteRow.get(n));
				} else if (columnName.equals("tradetime")) {
					LocalDateTime quoteDate = Instant.parse((CharSequence) quoteRow.get(n)).atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();
					returnRow.put("dtLastUpdate", quoteDate);
					returnRow.put("dt", quoteDate);
				} else {
					columnValue = Double.parseDouble((String) quoteRow.get(n));
					returnRow.put(columnName, columnValue);
				}
			}
			return returnRow;
		}
		return null;
	}

	/**
	 * Creates an authorised Credential object.
	 * 
	 * @param HTTP_TRANSPORT The network HTTP Transport.
	 * @return An authorised Credential object.
	 * @throws IOException If the credentials.json file cannot be found.
	 */
	private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
		// Load client secrets.
		InputStream in = GoogleSheetsQuote.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
		if (in == null) {
			throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
		}
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

		// Build flow and trigger user authorisation request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
				.setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH))).setAccessType("offline").build();
		LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
		return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
	}
}
