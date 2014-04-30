package edu.virginia.lib.ld2solr.api;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * A simple class to associate a {@link Model} with an URI. It is assumed that
 * the triples in the model primarily concern the resource identified by the
 * URI.
 * 
 * @author ajs6f
 * 
 */
public class IdentifiedModel {

	public final Model model;

	public final Resource uri;

	/**
	 * @param m
	 * @param uri
	 */
	public IdentifiedModel(final Resource u, final Model m) {
		this.model = m;
		this.uri = u;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return uri.getURI() + " => \n " + model.toString();
	}

}
