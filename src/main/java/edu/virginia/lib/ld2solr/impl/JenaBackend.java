/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import static com.hp.hpl.jena.query.ReadWrite.READ;

import org.apache.marmotta.ldpath.backend.jena.GenericJenaBackend;

import com.hp.hpl.jena.query.Dataset;
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
	 * @param model
	 *            the model with which to back this cache.
	 */
	public JenaBackend(final Model m) {
		super(m);
		this.model = m;
	}

	public static JenaBackend with(final Dataset d) {
		d.begin(READ);
		final Model m = d.getDefaultModel();
		d.end();
		return new JenaBackend(m);
	}

	/**
	 * @return the model in use
	 */
	public Model model() {
		return model;
	}

}
