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
	 * @param m
	 *            the model with which to back this cache.
	 */
	public JenaBackend(final Model m) {
		super(m);
		this.model = m;
	}

	/**
	 * A factory method to use when operating over a (transactional)
	 * {@link Dataset}. The default model in the {@link Dataset} is used.
	 * 
	 * @param d
	 *            The {@link Dataset} on which to draw.
	 * @return a {@link JenaBackend} backed by d
	 */
	public static JenaBackend with(final Dataset d) {
		d.begin(READ);
		final Model m = d.getDefaultModel();
		d.end();
		return new JenaBackend(m);
	}

	/**
	 * @return the {@link Model} backing this {@link JenaBackend}
	 */
	public Model model() {
		return model;
	}
}
