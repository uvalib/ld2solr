package edu.virginia.lib.ld2solr.spi;

import com.hp.hpl.jena.rdf.model.Model;

import edu.virginia.lib.ld2solr.spi.Stage.Acceptor;

/**
 * An {@link Acceptor} that takes RDF (packaged in {@link Model}s) and loads it
 * into a cache.
 * 
 * @author ajs6f
 * 
 */
public interface CacheLoader extends Acceptor<Model, Void> {
}
