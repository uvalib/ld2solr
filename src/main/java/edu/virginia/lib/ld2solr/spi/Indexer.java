package edu.virginia.lib.ld2solr.spi;

import com.google.common.base.Function;

/**
 * An {@link Indexer} produces indexer heads from a particular store or source
 * of Linked Data.
 * 
 * @author ajs6f
 * @param <IndexerHead>
 *            the type of {@link IndexingTransducer} to be constructed
 */
public interface Indexer<IndexerHead extends IndexingTransducer> extends Function<String, IndexerHead> {
}
