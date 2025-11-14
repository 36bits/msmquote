package uk.co.pueblo.msm.msmquote;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parent class for MSN quote sources.
 */
abstract class MsnSource extends QuoteSource {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsnSource.class);
	static final Properties PROPS;

	static {
		Properties props = null;
		try {
			props = loadProperties("MsnSource.properties");
		} catch (Exception e) {
			LOGGER.debug("Exception occured!", e);
			LOGGER.fatal("Failed to load properties: {}", e.getMessage());
		}
		PROPS = props;
	}
}