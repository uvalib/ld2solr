package edu.virginia.lib.ld2solr.api;

import java.io.OutputStream;

/**
 * Represents the externally-meaningful output of an indexing operation,
 * containing both an identifier for the record and an {@link OutputStream} that
 * serializes the record.
 * 
 * @author ajs6f
 * 
 */
public interface OutputRecord {

	/**
	 * @return the identifier of the resource indexed in this record
	 */
	public String id();

	/**
	 * @return the record itself
	 */
	public byte[] record();

}
