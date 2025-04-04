package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uk.co.pueblo.msmcore.MsmCurrency;
import uk.co.pueblo.msmcore.MsmDb;
import uk.co.pueblo.msmcore.MsmDb.CliDatValue;
import uk.co.pueblo.msmcore.MsmSecurity;
import uk.co.pueblo.msmcore.MsmInstrumentException;

public class Update {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Update.class);
	private static final int STATUS_OK = 0;
	private static final int STATUS_FATAL = 3;

	public static void main(String[] args) {

		final Instant startTime = Instant.now();

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());
		int maxStatus = STATUS_OK;

		try {
			// Process command-line arguments
			if (args.length < 2) {
				throw new IllegalArgumentException("Usage: <filename> <password> [source]");
			}

			// Open Money database
			final MsmDb msmDb = new MsmDb(args[0], args[1]);

			try {
				// Instantiate Money objects
				final MsmSecurity msmSecurity = new MsmSecurity(msmDb);
				final MsmCurrency msmCurrency = new MsmCurrency(msmDb);

				// Instantiate quote object according to quote source
				final QuoteSource quoteSource;
				if (args.length == 2) {
					quoteSource = new YahooApiQuote("", msmSecurity.getSymbols(), msmCurrency.getSymbols());
				} else if (args[2].matches("^https://query[0-9].finance.yahoo.com/v[0-9]+/finance/quote.+")) {
					if (args[2].endsWith("symbols=") || args[2].endsWith("symbols=?")) {
						quoteSource = new YahooApiQuote(args[2], msmSecurity.getSymbols(), msmCurrency.getSymbols());
					} else {
						quoteSource = new YahooApiQuote(args[2]);
					}
				} else if (args[2].matches("^https://query[0-9].finance.yahoo.com/v[0-9]+/finance/chart.+")) {
					quoteSource = new YahooApiHist(args[2]);
				} else if (args[2].endsWith(".csv")) {
					quoteSource = new YahooCsvHist(args[2]);
				} else if (args.length == 4 && args[3].startsWith("hist ")) {
					quoteSource = new GoogleSheetsHist(args[2], args[3]);
				} else if (args.length == 4) {
					quoteSource = new GoogleSheetsQuote(args[2], args[3]);
				} else {
					throw new IllegalArgumentException("Unrecognised quote source");
				}

				// Get exit code from quote source
				maxStatus = quoteSource.getStatus();

				// Do update
				Map<String, String> quoteRow = new HashMap<>();
				while (!(quoteRow = quoteSource.getNext()).isEmpty()) {
					String quoteType = quoteRow.get("xType").toString();
					try {
						if (quoteType.equals("CURRENCY")) {
							msmCurrency.update(quoteRow); // update currency FX rates
						} else {
							msmSecurity.update(quoteRow); // update other security types
						}
					} catch (MsmInstrumentException e) {
						LOGGER.error(e.getMessage());
					}
				}

				// Post update processing
				msmSecurity.addNewRows(); // add any new rows to the SP table
				msmDb.updateCliDatVal(CliDatValue.OLUPDATE, LocalDateTime.now()); // update online update time-stamp

				// Output update summaries to log and set status
				int updateStatus;
				if ((updateStatus = msmSecurity.printSummary()) > maxStatus) {
					maxStatus = updateStatus;
				}
				if ((updateStatus = msmCurrency.printSummary()) > maxStatus) {
					maxStatus = updateStatus;
				}

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occurred!", e);
				maxStatus = STATUS_FATAL;
			} finally {
				msmDb.closeDb(); // close Money database
			}

		} catch (Exception e) {
			LOGGER.fatal(e);
			LOGGER.debug("Exception occurred!", e);
			maxStatus = STATUS_FATAL;
		} finally {
			LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
			System.exit(maxStatus);
		}
	}
}