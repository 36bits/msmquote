package uk.co.pueblo.msmquote.msm;

import java.io.IOException;
import java.util.Collections;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Column;

public class CliDatTable extends MsmTable {

	// Define CLI_DAT rows
	public enum CliDatRow {
		FILENAME(65541, 8, "rgbVal"),
		OLUPDATE(917505, 7, "dtVal");

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
	 * Constructor for the CLI_DAT table.
	 * 
	 * @param msmDb the opened MS Money file
	 * @throws IOException
	 */
	public CliDatTable(Database msmDb) throws IOException {
		super(msmDb, "CLI_DAT");		
		return;
	}	

	/** 
	 * Updates a value in the CLI_DAT table.
	 * 
	 * @param	name	the name of the row to be updated
	 * @param	newVal		the new value
	 * @return			true if successful, otherwise false
	 * @throws	IOException
	 */
	public boolean update(CliDatRow name, Object newVal) throws IOException {
		boolean found = msmCursor.findFirstRow(Collections.singletonMap("idData", name.getIdData()));
		if (found) {
			Column cliDatCol = msmTable.getColumn("oft");
			msmCursor.setCurrentRowValue(cliDatCol, name.getOft());
			cliDatCol = msmTable.getColumn(name.getValCol());
			msmCursor.setCurrentRowValue(cliDatCol, newVal);			
			return true;
		}
		return false;
	}
}