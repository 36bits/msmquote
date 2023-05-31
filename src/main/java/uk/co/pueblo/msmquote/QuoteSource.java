package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

abstract class QuoteSource {

	// Constants
	static final Properties PROPS = new Properties();
	static final int SOURCE_OK = 0;
	static final int SOURCE_WARN = 1;
	static final int SOURCE_ERROR = 2;
	static final int SOURCE_FATAL = 3;
	
	// Class variables
	private static int finalStatus = SOURCE_OK;
		
	abstract Map<String, String> getNext() throws IOException;
	
	static int getStatus() {
		return finalStatus;
	}

	static void setStatus(int status) {
		if (status > finalStatus) {
			finalStatus = status;
		}
		return;
	}
}