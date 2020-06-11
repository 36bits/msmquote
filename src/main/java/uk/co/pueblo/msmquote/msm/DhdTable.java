package uk.co.pueblo.msmquote.msm;

import java.io.IOException;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;

public class DhdTable extends MsmTable {

	// Define DHD table columns
	public enum DhdColumn {
		BASE_CURRENCY("hcrncDef");
		
		private final String column;

		DhdColumn(String column) {
			this.column = column;
		}

		public String getName() {
			return column;
		}
	}	

	/**
	 * Constructor for the defaults(?) table.
	 * 
	 * @param msmDb the opened MS Money file
	 * @throws IOException
	 */
	public DhdTable(Database msmDb) throws IOException {
		super(msmDb, "DHD");
		return;
	}    

	/** 
	 * Gets the value of the given column.
	 * 
	 * @param	dhdCol	the name of the column
	 * @return			the hcrnc
	 * @throws	IOException
	 */
	public int getValue(String dhdCol) throws IOException {
		Row row = msmTable.getNextRow();
		return (int) row.get(dhdCol);        
	}
}