/**
 * 
 */
package edu.virginia.lib.ld2solr.spi;

import java.io.IOException;

import edu.virginia.lib.ld2solr.api.OutputRecord;

/**
 * Accepts {@link OutputRecord}s and does something useful with them.
 * 
 * @author ajs6f
 * 
 */
public interface RecordSink {

	/**
	 * Do something interesting with this {@link OutputRecord}.
	 * 
	 * @param record
	 * @throws IOException
	 */
	public void accept(final OutputRecord record) throws IOException;

	/**
	 * A {@link RecordSink} that persists its inputs.
	 * 
	 * @author ajs6f
	 * 
	 */
	public static interface RecordPersister extends RecordSink {
	}

}
