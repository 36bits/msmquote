package uk.co.pueblo.msmquote;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

public class QuoteSummary {

	// Instance variables
	private Map<String, int[]> summary;

	// Define summary types
	public enum SummaryType {
		PROCESSED, WARNING
	}
	
	public QuoteSummary() {
		summary = new HashMap<>();
	}

	public void inc(String key, SummaryType type) {
		summary.putIfAbsent(key, new int[] {0, 0});
		int[] count = summary.get(key);
		count[type.ordinal()]++;
		summary.put(key, count);
		return;
	}

	public void log(Logger logger) {
		summary.forEach((key, count) -> {
			logger.info("Summary for quote type {}: processed = {}, warnings = {}", key, count[0], count[1]);
		});
	}
}
