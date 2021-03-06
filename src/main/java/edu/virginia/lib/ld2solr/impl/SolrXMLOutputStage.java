package edu.virginia.lib.ld2solr.impl;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Maps.transformEntries;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.util.Collection;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Maps.EntryTransformer;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.OutputStage;
import edu.virginia.lib.ld2solr.spi.ThreadedStage;

/**
 * An {@link OutputStage} that produces records in Solr XML form.
 * 
 * @author ajs6f
 * 
 */
public class SolrXMLOutputStage extends ThreadedStage<SolrXMLOutputStage, OutputRecord> implements OutputStage {

	// TODO make index-time boost somehow adjustable, or something
	public static final Long INDEX_TIME_BOOST = 1L;

	private static final Logger log = getLogger(SolrXMLOutputStage.class);

	/*
	 * (non-Javadoc)
	 * 
	 * @see Acceptor#accept(java.lang.Object)
	 */
	@Override
	public void accept(final NamedFields fields) {
		final SolrInputDocument record = wrap.apply(fields);
		next(new OutputRecord() {

			@Override
			public String id() {
				return fields.id();
			}

			@Override
			public byte[] record() {
				try {
					return new UpdateRequest().add(record).getXML().getBytes();
				} catch (final IOException e) {
					throw propagate(e);
				}
			}
		});
	}

	/**
	 * Produces a {@link SolrInputDocument} from a {@link NamedFields} input.
	 */
	protected static Function<NamedFields, SolrInputDocument> wrap = new Function<NamedFields, SolrInputDocument>() {

		@Override
		public SolrInputDocument apply(final NamedFields fields) {
			return new SolrInputDocument(transformEntries(fields, collection2solrInputField));
		}
	};

	/**
	 * Creates a {@link SolrInputField} from a single named field.
	 */
	private static EntryTransformer<String, Collection<String>, SolrInputField> collection2solrInputField = new EntryTransformer<String, Collection<String>, SolrInputField>() {

		@Override
		public SolrInputField transformEntry(final String key, final Collection<String> input) {
			final SolrInputField field = new SolrInputField(key);
			for (final String value : input) {
				log.trace("Adding value: {} to field: {}", value, key);
				field.addValue(value, INDEX_TIME_BOOST);
			}
			return field;
		}
	};
}
