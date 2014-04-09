/**
 * 
 */
package edu.virginia.lib.ld2solr.spi;

import com.google.common.base.Function;

/**
 * An {@link Indexer} produces {@link IndexerHeads} from a particular store of
 * Linked Data.
 * 
 * @author ajs6f
 * 
 */
public interface Indexer<T extends IndexingTransducer> extends Function<String, T> {

}
