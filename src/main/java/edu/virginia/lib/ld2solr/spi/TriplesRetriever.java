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

	public static class RetrievalException extends RuntimeException {

		public RetrievalException(final String msg) {
			super(msg);
		}

		private static final long serialVersionUID = 1L;
		
	}
	
}