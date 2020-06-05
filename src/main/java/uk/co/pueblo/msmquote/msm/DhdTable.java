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
	 * Constructor.
	 * 
	 * @param	mnyDb
	 * @throws IOException
	 */
	public DhdTable(Database msmDb) throws IOException {
		super(msmDb, "DHD");
		return;
	}    

	/** 
	 * Get the hcrnc of the base currency.
	 * 
	 * @return				the hcrnc
	 */
	public int getValue(String dhdCol) throws IOException {
		Row row = msmTable.getNextRow();
		return (int) row.get(dhdCol);        
	}
}