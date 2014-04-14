/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import org.apache.marmotta.ldpath.backend.jena.GenericJenaBackend;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * We subclass {@link GenericJenaBackend} in order to expose the Jena
 * {@link Model} inside it, because we wish to manipulate that model directly.
 * 
 * @author ajs6f
 * 
 */
public class JenaBackend extends GenericJenaBackend {

	private final Model model;

	/**
	 * @param m
	 *            the model with which to back this cache.
	 */
	public JenaBackend(final Model m) {
		super(m);
		this.model = m;
	}

	/**
	 * @return the model in use
	 */
	public Model model() {
		return model;
	}

}
