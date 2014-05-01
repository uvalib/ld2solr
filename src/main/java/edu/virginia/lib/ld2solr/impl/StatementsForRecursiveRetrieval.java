package edu.virginia.lib.ld2solr.impl;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterators.any;
import static com.hp.hpl.jena.vocabulary.OWL.ObjectProperty;
import static com.hp.hpl.jena.vocabulary.RDF.type;
import static edu.virginia.lib.ld2solr.spi.CacheAssembler.traversableForRecursiveRetrieval;

import com.google.common.base.Predicate;
import com.hp.hpl.jena.ontology.OntProperty;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Selector;
import com.hp.hpl.jena.rdf.model.Statement;

/**
 * A {@link Selector} that finds {@link Statement}s that represent links to be
 * traversed for recursive retrieval, based on the ontology supplied for that
 * purpose.
 * 
 * @author ajs6f
 * 
 */
public class StatementsForRecursiveRetrieval implements Selector {

	/**
	 * A version of the "traversableForRecursiveRetrieval" property that is
	 * local to this ontology-enabled {@link Model}. The assumption is that all
	 * {@link Statement}s tested by this {@link Selector} will come from the
	 * same model.
	 */
	private final Predicate<OntProperty> testForRetrievalQuality;

	/**
	 * @param m
	 *            the {@link Model} over which this {@link Selector} will
	 *            operate
	 */
	public StatementsForRecursiveRetrieval(final Model m) {
		final OntProperty localTraversableForRecursiveRetrieval = m
				.createProperty(traversableForRecursiveRetrieval.getURI()).addProperty(type, ObjectProperty)
				.as(OntProperty.class);
		this.testForRetrievalQuality = equalTo(localTraversableForRecursiveRetrieval);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Selector#test(Statement)
	 */
	@Override
	public boolean test(final Statement s) {
		// a property that is not URI-valued cannot be traversed
		if (!s.getObject().isURIResource()) {
			return false;
		}
		// we have an URI-valued property in hand. now we
		// must test whether our property is a subproperty of
		// "traversableForRecursiveRetrieval"
		if (s.getPredicate().canAs(OntProperty.class)) {
			final OntProperty predicate = s.getPredicate().as(OntProperty.class);
			if (any(predicate.listSuperProperties(), testForRetrievalQuality)) {
				return true;
			}
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Selector#isSimple()
	 */
	@Override
	public boolean isSimple() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Selector#getSubject()
	 */
	@Override
	public Resource getSubject() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Selector#getPredicate()
	 */
	@Override
	public Property getPredicate() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Selector#getObject()
	 */
	@Override
	public RDFNode getObject() {
		return null;
	}

}
