package uk.co.pueblo.msmquote.msm;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;

public class Db extends DatabaseBuilder {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(Db.class);

	// Instance variables
	private Database db;

	// Constructor
	public Db(String fileName, String password) throws IOException {

		// Create lock file
		String lockFileName = null;
		int i = fileName.lastIndexOf('.');
		if (i <= 0) {
			lockFileName = fileName;
		}
		else {
			lockFileName = fileName.substring(0, i);
		}
		File lockFile = new File(lockFileName + ".lrd");
		LOGGER.info("Creating lock file: {}", lockFile.getAbsolutePath());
		if (!lockFile.createNewFile()) {
			throw new FileAlreadyExistsException("Lock file already exists");
		}
		lockFile.deleteOnExit();

		// Open Money database
		File dbFile = new File(fileName);
		CryptCodecProvider cryptCp = null;

		if (password == null) {
			cryptCp = new CryptCodecProvider();
		}
		else {
			cryptCp = new CryptCodecProvider(password);
		}
		LOGGER.info("Opening Money file: {}", dbFile.getAbsolutePath());
		db = new DatabaseBuilder(dbFile).setCodecProvider(cryptCp).open();
		db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
		return;
	}

	public Database getDb() {
		return db;
	}

	public void closeDb() throws IOException {
		LOGGER.info("Closing Money file: {}", db.getFile());
		db.close();
		return;
	}
}