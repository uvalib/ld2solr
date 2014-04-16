/**
 * 
 */
package edu.virginia.lib.ld2solr.impl;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Maps.transformEntries;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps.EntryTransformer;

import edu.virginia.lib.ld2solr.api.NamedFields;
import edu.virginia.lib.ld2solr.api.OutputRecord;
import edu.virginia.lib.ld2solr.spi.AbstractStage;
import edu.virginia.lib.ld2solr.spi.OutputStage;

/**
 * @author ajs6f
 * 
 */
public class SolrLDOutputStage extends AbstractStage<OutputRecord> implements OutputStage<SolrLDOutputStage> {

	@Override
	public void accept(final NamedFields fields) {
		final SolrInputDocument record = SolrWrappedNamedFields.wrap.apply(fields);
		nextStage.accept(new OutputRecord() {

			@Override
			public String id() {
				return fields.id();
			}

			@Override
			public byte[] record() {
				try (StringWriter w = new StringWriter()) {
					new UpdateRequest().add(record).writeXML(w);
					return w.toString().getBytes();
				} catch (final IOException e) {
					throw propagate(e);
				}
			}
		});
	}

	/**
	 * Exports a {@link NamedFields} instance as a {@link SolrDocument}.
	 * 
	 * @author ajs6f
	 * 
	 */
	public static class SolrWrappedNamedFields implements Supplier<SolrInputDocument> {

		private final NamedFields fields;

		// TODO make index-time boost somehow adjustable, or something
		public static final Long INDEX_TIME_BOOST = 1L;

		private static final Logger log = getLogger(SolrWrappedNamedFields.class);

		@Override
		public SolrInputDocument get() {
			log.trace("Constructing new SolrInputDocument...");
			return new SolrInputDocument(transformEntries(fields, collection2solrInputField));
		}

		/**
		 * @param namedFields
		 *            the fields to wrap for export
		 */
		public SolrWrappedNamedFields(final NamedFields namedFields) {
			this.fields = namedFields;
		}

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

		public static Function<NamedFields, SolrInputDocument> wrap = new Function<NamedFields, SolrInputDocument>() {

			@Override
			public SolrInputDocument apply(final NamedFields fields) {
				return new SolrWrappedNamedFields(fields).get();
			}
		};

	}
}
