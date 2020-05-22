package uk.co.pueblo.msmquote;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;

import uk.co.pueblo.msmquote.MsmCliDatTable.IdData;
import uk.co.pueblo.msmquote.MsmDhdTable.DhdColumn;

public class Update {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Update.class);

	private enum ExitCode {
		OK(0), WARNING(1), ERROR(2);

		private final int code;

		ExitCode(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}
	}

	public static void main(String[] args) {

		LOGGER.info("Version {}", Update.class.getPackage().getImplementationVersion());

		Instant startTime = Instant.now();
		int exitCode = ExitCode.OK.getCode();
		MsmDb db = null;

		try {	    	
			// Process command-line arguments
			String password = null;
			String sourceArg = null;

			if (args.length == 3) {
				password = args[1];
				sourceArg = args[2];
			} else if (args.length == 2) {
				sourceArg = args[1];
			} else {
				throw new IllegalArgumentException("Usage: filename [password] source");
			}

			// Open Money database
			Database openedDb = null;			
			db = new MsmDb(args[0], password);
			openedDb = db.getDb();

			try {
				// Instantiate table objects needed to process source quote type
				MsmSecTable secTable = new MsmSecTable(openedDb);
				MsmCrncTable crncTable = new MsmCrncTable(openedDb);
				MsmDhdTable dhdTable = new MsmDhdTable(openedDb);
				MsmCntryTable cntryTable = new MsmCntryTable(openedDb);

				// Process quote source types
				YahooQuote yahooQuote = null;

				if (sourceArg.startsWith("https://") || sourceArg.startsWith("http://")) {
					if (sourceArg.endsWith("symbols=")  || sourceArg.endsWith("symbols=?")) {
						yahooQuote = new YahooApiQuote(sourceArg, secTable.getSymbols(cntryTable), crncTable.getIsoCodes(dhdTable.getValue(DhdColumn.BASE_CURRENCY.getName())));
					} else {
						yahooQuote = new YahooApiQuote(sourceArg);
					}
				} else if (sourceArg.endsWith(".csv")) {
					yahooQuote = new YahooCsvQuote(sourceArg);
				} else {
					throw new IllegalArgumentException("Unrecogonised quote source");
				}

				if (!yahooQuote.isQuery()) {
					// Instantiate table objects needed to process quote data
					MsmSpTable spTable = new MsmSpTable(openedDb);
					MsmFxTable fxTable = new MsmFxTable(openedDb);
					MsmCliDatTable cliDatTable = new MsmCliDatTable(openedDb);

					// Now update quote data in Money database
					int hsec;
					String currencyPair;
					String[] isoCodes = {"", ""};
					int[] hcrncs = {0, 0};
					Map<String, Object> quoteRow = new HashMap<>();				
					while (true) {
						if ((quoteRow = yahooQuote.getNext()) == null) {
							break;
						}
						if (quoteRow.containsKey("xError")) {
							exitCode = ExitCode.WARNING.getCode();
						}
						if (quoteRow.get("dPrice") != null) {
							// Update stock quote data
							if((hsec = secTable.update(quoteRow)) != -1) {
								spTable.update(quoteRow, hsec);
							} else {
								exitCode = ExitCode.WARNING.getCode();
							}
						} else if (quoteRow.get("rate") != null) {
							// Get hcrncs of currency pair
							currencyPair = (String) quoteRow.get("symbol");
							isoCodes[0] = currencyPair.substring(0, 3);
							isoCodes[1] = currencyPair.substring(3, 6);
							hcrncs = crncTable.getHcrncs(isoCodes);
							// Update exchange rate table
							if (!fxTable.update(hcrncs, (double) quoteRow.get("rate"))) {
								exitCode = ExitCode.WARNING.getCode();
							}
						} else {
							exitCode = ExitCode.WARNING.getCode();
						}
					} 

					// Add any new rows to the SP table
					spTable.addNewRows();

					// Update online update time-stamp
					cliDatTable.update(IdData.OLUPDATE.getCode(), IdData.OLUPDATE.getOft(), IdData.OLUPDATE.getColumn(), LocalDateTime.now());
				}

				} catch (Exception e) {
					LOGGER.fatal(e);
					LOGGER.debug("Exception occured!", e);
					exitCode = ExitCode.ERROR.getCode();
				}

				// Close Money database
				db.closeDb();

			} catch (Exception e) {
				LOGGER.fatal(e);
				LOGGER.debug("Exception occured!", e);
				exitCode = ExitCode.ERROR.getCode();
			}									
			LOGGER.info("Duration: {}", Duration.between(startTime, Instant.now()).toString());
			System.exit(exitCode);
		}
	}