/**
 * 
 */
package edu.virginia.lib.ld2solr.spi;

import java.util.Iterator;

import edu.virginia.lib.ld2solr.api.OutputRecord;

/**
 * Generates {@link OutputRecord}s.
 * 
 * @author ajs6f
 * 
 * @param <OutputType>
 * @param <IdentifierType>
 */
public interface OutputStage extends Iterator<OutputRecord> {

}
