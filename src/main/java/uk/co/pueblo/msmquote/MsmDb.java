package uk.co.pueblo.msmquote;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;

public class MsmDb extends DatabaseBuilder {

	// Constants
	private static final Logger LOGGER = LogManager.getLogger(MsmDb.class);

	// Instance variables
	private final Database db;

	// Constructor
	MsmDb(String fileName, String password) throws IOException {

		// Create lock file
		final String lockFileName;
		final int i = fileName.lastIndexOf('.');
		if (i <= 0) {
			lockFileName = fileName;
		} else {
			lockFileName = fileName.substring(0, i);
		}
		final File lockFile = new File(lockFileName + ".lrd");
		LOGGER.info("Creating lock file: {}", lockFile.getAbsolutePath());
		if (!lockFile.createNewFile()) {
			throw new FileAlreadyExistsException("Lock file already exists");
		}
		lockFile.deleteOnExit();

		// Open Money database
		final File dbFile = new File(fileName);
		final CryptCodecProvider cryptCp;

		if (password.length() > 0) {
			cryptCp = new CryptCodecProvider(password);
		}
		else {
			cryptCp = new CryptCodecProvider();			
		}
		LOGGER.info("Opening Money file: {}", dbFile.getAbsolutePath());
		db = new DatabaseBuilder(dbFile).setCodecProvider(cryptCp).open();
		db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
		return;
	}

	Database getDb() {
		return db;
	}

	void closeDb() throws IOException {
		LOGGER.info("Closing Money file: {}", db.getFile());
		db.close();
		return;
	}
}