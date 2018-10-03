package uk.co.pueblo.msmquote;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

import org.apache.log4j.Logger;

import com.healthmarketscience.jackcess.CryptCodecProvider;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

public class MnyDb {
	static final Logger logger = Logger.getLogger(MnyDb.class);
		
	public static Database open(String fileName, String password) throws IOException {
				
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
		logger.info("Creating lock file: " + lockFile.getAbsolutePath());
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
		logger.info("Opening Money file: " + dbFile.getAbsolutePath());
		Database db = new DatabaseBuilder(dbFile)
				.setCodecProvider(cryptCp)
				.open();
				
	return db;
	}

	public static void close(Database db) throws IOException {
		logger.info("Closing Money file: " + db.getFile());
		db.close();
	}
}