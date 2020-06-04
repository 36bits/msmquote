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
	 * @param	csvFile
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
		String[] quoteMeta = tmp.substring(0, tmp.length() - 4).split("_");	// index 0 = symbol, index 1 = currency, index 2 = quote type
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
	 * Get the next row of quote data in the CSV file.
	 * 
	 * @return
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	@Override
	public Map<String, Object> getNext() throws IOException, NumberFormatException {

		Map<String, Object> quoteRow = new HashMap<>();

		while (true) {
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
			
			// SEC table columns
			quoteRow.put("xSymbol", symbol);				// xSymbol is used internally, not by MS Money
			// Assume dtLastUpdate is date of quote data in SEC row
			quoteRow.put("dtLastUpdate", quoteDate);

			// SP table columns
			quoteRow.put("dt", quoteDate);
			try {
				quoteRow.put("dOpen", Double.parseDouble(csvColumn[1]) / quoteDivisor);
				quoteRow.put("dHigh", Double.parseDouble(csvColumn[2]) / quoteDivisor);
				quoteRow.put("dLow", Double.parseDouble(csvColumn[3]) / quoteDivisor);
				quoteRow.put("dPrice", Double.parseDouble(csvColumn[4]) / quoteDivisor);
				quoteRow.put("vol", Long.parseLong(csvColumn[6]));
			} catch (NumberFormatException e) {
				LOGGER.warn(e);
				LOGGER.debug("Exception occured!", e);
				quoteRow.put("xError", null);
				continue;
			}
			
			quoteSummary.inc(quoteType, SummaryType.PROCESSED);
			return quoteRow;
		}
	}

	/**
	 * @return
	 */
	@Override
	public boolean isQuery() {
		return isQuery;		
	}
	
}