package uk.co.pueblo.msmquote.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import uk.co.pueblo.msmquote.source.QuoteSummary.SummaryType;

public class YahooCsvHist extends YahooQuote {

	// Instance variables
	private BufferedReader csvBr;
	private String symbol;
	private int quoteDivisor;
	private String quoteType;

	/**
	 * Constructor for CSV file quote data source.
	 * 
	 * @param	fileName	the name of the CSV file
	 * @throws IOException 
	 */
	public YahooCsvHist(String fileName) throws IOException {
		File csvFile = new File(fileName);
		csvBr = new BufferedReader(new FileReader(csvFile));
		if (!csvBr.readLine().equals("Date,Open,High,Low,Close,Adj Close,Volume")) {
			LOGGER.warn("Yahoo CSV header not found in file {}", csvFile);
			csvBr.close();
		}				

		// Get quote meta-data from CSV file name
		String tmp = csvFile.getName();
		String[] quoteMeta = tmp.substring(0, tmp.length() - 4).split("_");		// index 0 = symbol, index 1 = currency, index 2 = quote type
		symbol = quoteMeta[0];
		quoteType = quoteMeta[2];

		// Set quote divisor according to currency
		quoteDivisor = 1;
		String quoteDivisorProp = baseProps.getProperty("divisor." + quoteMeta[1] + "." + quoteMeta[2]);
		if (quoteDivisorProp != null) {
			quoteDivisor = Integer.parseInt(quoteDivisorProp);
		}

		isQuery = false;
		quoteSummary = new QuoteSummary();
	}

	/**
	 * Gets the next row of quote data from the CSV file.
	 * 
	 * @return	the quote row or null if no more data 
	 * @throws IOException
	 */
	@Override
	public Map<String, Object> getNext() throws IOException {

		Map<String, Object> quoteRow = new HashMap<>();

		// Get next row from CSV file
		String csvRow = csvBr.readLine();
		if (csvRow == null) {
			// End of file
			csvBr.close();
			quoteSummary.log(LOGGER);
			return null;
		}
		String[] csvColumn = csvRow.split(",");

		// Get quote date
		LocalDateTime quoteDate = LocalDateTime.parse(csvColumn[0] + "T00:00:00").atZone(SYS_ZONE_ID).toLocalDate().atStartOfDay();
		//LocalDateTime quoteDate = LocalDateTime.parse(csvColumn[0] + "T00:00:00").toLocalDate().atStartOfDay();


		// Build columns for msmquote internal use
		quoteRow.put("xSymbol", symbol);			

		// Build SEC table columns
		quoteRow.put("dtLastUpdate", quoteDate);			// TODO Confirm assumption that dtLastUpdate is date of quote data in SEC row

		// SP table columns
		quoteRow.put("dt", quoteDate);
		try {
			String prop;
			double value;
			for (int n = 1; n < csvColumn.length; n++) {
				if ((prop = baseProps.getProperty("hist.csv." + n)) != null) {
					value = Double.parseDouble(csvColumn[n]);
					// Process adjustments
					if (Boolean.parseBoolean(baseProps.getProperty("divide." + prop))) {
						value = value / quoteDivisor;
					}
					// Now put key and value to quote row
					LOGGER.debug("Key = {}, value = {}", prop, value);
					if (prop.substring(0, 1).equals("d")) {
						quoteRow.put(prop, value);
					} else {
						quoteRow.put(prop, (long) value);
					}
				}
			}
		} catch (NumberFormatException e) {
			LOGGER.warn(e);
			LOGGER.debug("Exception occured!", e);
			quoteRow.put("xError", null);
		}

		quoteSummary.inc(quoteType, SummaryType.PROCESSED);
		return quoteRow;
	}
}