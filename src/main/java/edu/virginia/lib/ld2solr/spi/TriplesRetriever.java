package edu.virginia.lib.ld2solr.spi;

import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * Creates retrieval tasks from {@link Resource}s.
 * 
 * @author ajs6f
 * 
 */
public interface TriplesRetriever extends Function<Resource, Callable<Model>> {
}