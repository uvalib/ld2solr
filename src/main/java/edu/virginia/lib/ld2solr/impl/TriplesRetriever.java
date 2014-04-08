package edu.virginia.lib.ld2solr.impl;

import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * @author ajs6f
 * 
 */
public class TriplesRetriever {

	private Model model;

	public void load(Resource uri) {
		RDFDataMgr.read(model, uri.getURI());
	}
}
