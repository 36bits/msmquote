package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import uk.co.pueblo.msmcore.MsmCurrency;
import uk.co.pueblo.msmcore.MsmDb;
import uk.co.pueblo.msmcore.MsmDb.CliDatValue;
import uk.co.pueblo.msmcore.MsmSecurity;
import uk.co.pueblo.msmcore.MsmInstrument.UpdateStatus;
import uk.co.pueblo.msmcore.MsmInstrumentException;

public class Update {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Update.class);

	public static void main(String[] args) {

		final Instant startTime = Instant.now();
		Level maxLevel = Level.INFO;
		Map<String, int[]> summary = new HashMap<>();

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());

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

				// Get level from quote source
				maxLevel = quoteSource.getStatus().level;

				// Do update
				Map<String, String> quoteRow = new HashMap<>();
				while (!(quoteRow = quoteSource.getNext()).isEmpty()) {
					String quoteType = quoteRow.get("xType").toString();
					try {
						UpdateStatus updateStatus;
						if (quoteType.equals("CURRENCY")) {
							updateStatus = msmCurrency.update(quoteRow); // update currency FX rates
							incSummary(quoteType, updateStatus, summary);
						} else {
							updateStatus = msmSecurity.update(quoteRow); // update other security types
							incSummary(quoteType, updateStatus, summary);
						}
						if (updateStatus.level.isMoreSpecificThan(maxLevel)) {
							maxLevel = updateStatus.level;
						}
					} catch (MsmInstrumentException e) {
						incSummary(quoteType, e.getUpdateStatus(), summary);
						LOGGER.log(e.getUpdateStatus().level, e.getMessage());
						if (e.getUpdateStatus().level.isMoreSpecificThan(maxLevel)) {
							maxLevel = e.getUpdateStatus().level;
						}
					}
				}

				// Post update processing
				msmSecurity.addNewRows(); // add any new rows to the SP table
				msmDb.updateCliDatVal(CliDatValue.OLUPDATE, LocalDateTime.now()); // update online update time-stamp

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occurred!", e);
				maxLevel = Level.FATAL;
			} finally {
				msmDb.closeDb(); // close Money database
			}

		} catch (Exception e) {
			LOGGER.fatal(e);
			LOGGER.debug("Exception occurred!", e);
			maxLevel = Level.FATAL;
		} finally {
			logSummary(summary);
			LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
			System.exit(4 - maxLevel.intLevel() / 100);
		}
	}

	private static void incSummary(String quoteType, UpdateStatus updateStatus, Map<String, int[]> summary) {
		summary.putIfAbsent(quoteType, new int[UpdateStatus.size]); // OK, warning, error, fatal, skipped, stale
		int[] count = summary.get(quoteType);
		count[updateStatus.index]++;
		summary.put(quoteType, count);
		return;
	}

	private static void logSummary(Map<String, int[]> summary) {
		summary.forEach((key, count) -> {
			StringJoiner logSj = new StringJoiner(", ");
			int sum = 0;
			for (UpdateStatus updateStatus : UpdateStatus.values()) {
				if (updateStatus.label != null) {
					logSj.add(updateStatus.label + count[updateStatus.index]);
					sum += count[updateStatus.index];
				}
			}
			LOGGER.info("Summary for quote type {}: processed={} [{}]", key, sum, logSj.toString());
		});
		return;
	}
}