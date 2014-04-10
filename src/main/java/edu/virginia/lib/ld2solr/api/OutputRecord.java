package edu.virginia.lib.ld2solr.api;

import java.io.OutputStream;

/**
 * Represents the final output of an indexing operation, containing both an
 * identifier for the record and an {@link OutputStream} that serializes the
 * record.
 * 
 * @author ajs6f
 * 
 */
public interface OutputRecord {

	public String id();

	public byte[] record();

}
