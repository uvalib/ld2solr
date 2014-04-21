package edu.virginia.lib.ld2solr.spi;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

/**
 * A {@link Stage} of workflow that generates {@link OutputRecord}s.
 * 
 * @author ajs6f
 * 
 * @param <OutputType>
 */
public interface OutputStage extends Acceptor<NamedFields, OutputRecord> {

}
