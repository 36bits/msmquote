package uk.co.pueblo.msmquote.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class YahooCsvHist implements Quote {
	
	// Constants
	private static final Logger LOGGER = LogManager.getLogger(YahooCsvHist.class);
	private static final String BASE_PROPS = "YahooQuote.properties";	

	// Class variables
	private static Properties props;

	// Instance variables
	private BufferedReader csvBr;
	private String symbol;
	private int quoteDivisor;
	private int quoteIndex;
	private boolean isQuery;
	
	static {
		try {
			// Set up properties
			InputStream propsIs = YahooCsvHist.class.getClassLoader().getResourceAsStream(BASE_PROPS);
			Properties props = new Properties();
			props.load(propsIs);
		} catch (IOException e) {
			LOGGER.fatal(e);
		}
	}
	
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

		// Get investment symbol from CSV file name
		String tmp = csvFile.getName();
		int tmpLen = tmp.length();
		symbol = tmp.substring(0, tmpLen - 8);

		// Set quote divisor according to currency
		String quoteDivisorProp = props.getProperty("quoteDivisor." + tmp.substring(tmpLen - 7, tmpLen - 4));
		if (quoteDivisorProp != null) {
			quoteDivisor = Integer.parseInt(quoteDivisorProp);
		}
		
		quoteIndex = 0;
		isQuery = false;
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
				LOGGER.info("Quotes processed = {}", quoteIndex);
				return null;
			}
			String[] csvColumn = csvRow.split(",");

			// Get quote date
			LocalDate quoteDate = LocalDate.parse(csvColumn[0]);

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
			
			quoteIndex++;			
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