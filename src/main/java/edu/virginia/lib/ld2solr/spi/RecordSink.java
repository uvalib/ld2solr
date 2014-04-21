package edu.virginia.lib.ld2solr.spi;

import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

/**
 * Accepts {@link OutputRecord}s and does something useful with them.
 * 
 * @author ajs6f
 * 
 */
public interface RecordSink extends Acceptor<OutputRecord, OutputRecord> {

	/**
	 * A {@link RecordSink} that persists its inputs.
	 * 
	 * @author ajs6f
	 * 
	 */
	public static interface RecordPersister extends RecordSink {

		/**
		 * @param location
		 *            the location of the persisted records
		 * @return the {@link RecordPersister} for continued operation
		 */
		public RecordPersister location(String location);

		/**
		 * @return the location of the persisted records
		 */
		public String location();
	}

}
