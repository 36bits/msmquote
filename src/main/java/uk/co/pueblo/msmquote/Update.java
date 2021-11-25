package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;

import uk.co.pueblo.msmquote.MsmCommon.CliDatRow;
import uk.co.pueblo.msmquote.MsmCommon.DhdColumn;

public class Update {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Update.class);
	private static final int EXIT_OK = 0;
	//private static final int EXIT_WARN = 1;
	private static final int EXIT_ERROR = 2;

	public static void main(String[] args) {

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());

		int exitCode, finalExitCode = EXIT_OK;
		final Instant startTime = Instant.now();
		final MsmDb db;

		try {
			// Process command-line arguments
			if (args.length < 3) {
				throw new IllegalArgumentException("Usage: filename password source");
			}

			// Open Money database
			db = new MsmDb(args[0], args[1]);
			final Database openedDb = db.getDb();

			try {
				// Instantiate objects needed to process quote source types
				final MsmCommon msmCore = new MsmCommon(openedDb);
				final MsmSecurity msmSecurity = new MsmSecurity(openedDb);
				final MsmCurrency msmCurrency = new MsmCurrency(openedDb);

				// Instantiate quote object according to quote source
				final QuoteSource quoteSource;

				if (args[2].contains("finance.yahoo.com/v7/finance/quote")) {
					if (args[2].endsWith("symbols=") || args[2].endsWith("symbols=?")) {
						quoteSource = new YahooApiQuote(args[2], msmSecurity.getSymbols(msmCore), msmCurrency.getIsoCodes(msmCore.getDhdVal(DhdColumn.BASE_CURRENCY.getName())));
					} else {
						quoteSource = new YahooApiQuote(args[2]);
					}
				} else if (args[2].contains("finance.yahoo.com/v7/finance/chart")) {
					quoteSource = new YahooApiHist(args[2]);
				} else if (args[2].endsWith(".csv")) {
					quoteSource = new YahooCsvHist(args[2]);
				} else if (args.length == 4) {
					quoteSource = new GoogleSheetsQuote(args[2], args[3]);
				} else {
					throw new IllegalArgumentException("Unrecognised quote source");
				}

				// Update
				if (!quoteSource.isQuery()) {
					Map<String, Object> quoteRow = new HashMap<>();
					while ((quoteRow = quoteSource.getNext()) != null) {
						if (quoteRow.containsKey("xType")) {
							if ((quoteRow.get("xType")).toString().equals("CURRENCY")) {
								// Update exchange rate
								if ((exitCode = msmCurrency.update(quoteRow)) > finalExitCode) {
									finalExitCode = exitCode;
								}
								continue;
							}
						}
						// All other quote types
						if ((exitCode = msmSecurity.update(quoteRow)) > finalExitCode) {
							finalExitCode = exitCode;
						}
					}

					// Add any new rows to the SP table
					msmSecurity.addNewSpRows();

					// Update online update time-stamp
					msmCore.updateCliDatVal(CliDatRow.OLUPDATE, LocalDateTime.now());

					// Print summaries
					msmSecurity.logSummary();
					msmCurrency.logSummary();
				}

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occurred!", e);
				finalExitCode = EXIT_ERROR;
			}

			// Close Money database
			db.closeDb();

		} catch (Exception e) {
			LOGGER.fatal(e);
			LOGGER.debug("Exception occurred!", e);
			finalExitCode = EXIT_ERROR;
		}
		LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
		System.exit(finalExitCode);
	}
}