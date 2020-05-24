package uk.co.pueblo.msmquote.source;

import java.io.IOException;
import java.util.Map;

/**
 * 
 *
 */
public interface Quote {
	public Map<String, Object> getNext() throws IOException;
	public boolean isQuery();
}