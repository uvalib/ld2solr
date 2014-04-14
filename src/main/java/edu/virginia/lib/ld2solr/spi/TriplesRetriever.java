package edu.virginia.lib.ld2solr.spi;

import java.io.IOException;

import org.apache.any23.extractor.ExtractionException;

import com.hp.hpl.jena.rdf.model.Resource;

public interface TriplesRetriever {

	/**
	 * @param uri
	 * @throws ExtractionException
	 * @throws IOException
	 */
	public abstract Resource load(Resource uri) throws IOException, ExtractionException;

}