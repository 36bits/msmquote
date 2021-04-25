package uk.co.pueblo.msmquote;

import java.io.IOException;
import java.util.Collections;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

class MsmCore {

	// Constants
	private static final String DHD_TABLE = "DHD";
	private static final String CLI_DAT_TABLE = "CLI_DAT";
	private static final String CNTRY_TABLE = "CNTRY";

	// Instance variables
	private Table dhdTable;
	private Table cliDatTable;
	private Table cntryTable;

	// DHD table columns
	enum DhdColumn {
		BASE_CURRENCY("hcrncDef");

		private final String column;

		DhdColumn(String column) {
			this.column = column;
		}

		public String getName() {
			return column;
		}
	}

	// CLI_DAT table rows
	enum CliDatRow {
		FILENAME(65541, 8, "rgbVal"), OLUPDATE(917505, 7, "dtVal");

		private final int idData;
		private final int oft;
		private final String valCol;

		CliDatRow(int idData, int oft, String valCol) {
			this.idData = idData;
			this.oft = oft;
			this.valCol = valCol;
		}

		public int getIdData() {
			return idData;
		}

		public int getOft() {
			return oft;
		}

		public String getValCol() {
			return valCol;
		}
	}

	/**
	 * Constructor.
	 * 
	 * @param msmDb the opened MS Money file
	 * @throws IOException
	 */
	MsmCore(Database msmDb) throws IOException {
		// Open the core tables
		dhdTable = msmDb.getTable(DHD_TABLE);
		cliDatTable = msmDb.getTable(CLI_DAT_TABLE);
		cntryTable = msmDb.getTable(CNTRY_TABLE);
		return;
	}

	/**
	 * Get the value of the given column in the DHD table.
	 * 
	 * @param dhdCol the name of the column
	 * @return the hcrnc
	 * @throws IOException
	 */
	int getDhdVal(String dhdCol) throws IOException {
		Row row = dhdTable.getNextRow();
		return (int) row.get(dhdCol);
	}

	/**
	 * Update a value in the CLI_DAT table.
	 * 
	 * @param name   the name of the row to be updated
	 * @param newVal the new value
	 * @return true if successful, otherwise false
	 * @throws IOException
	 */
	boolean updateCliDatVal(CliDatRow name, Object newVal) throws IOException {
		IndexCursor cursor = CursorBuilder.createCursor(cliDatTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("idData", name.getIdData()));
		if (found) {
			Column cliDatCol = cliDatTable.getColumn("oft");
			cursor.setCurrentRowValue(cliDatCol, name.getOft());
			cliDatCol = cliDatTable.getColumn(name.getValCol());
			cursor.setCurrentRowValue(cliDatCol, newVal);
			return true;
		}
		return false;
	}

	/**
	 * Get the country code for the given hcntry.
	 * 
	 * @param hcntry the hcntry to find the country code for
	 * @return the country code or null if not found
	 * @throws IOException
	 */
	String getCntryCode(int hcntry) throws IOException {
		IndexCursor cursor = CursorBuilder.createCursor(cntryTable.getPrimaryKeyIndex());
		boolean found = cursor.findFirstRow(Collections.singletonMap("hcntry", hcntry));
		if (found) {
			return (String) cursor.getCurrentRowValue(cntryTable.getColumn("szCode"));
		}
		return null;
	}
}