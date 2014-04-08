/**
 * 
 */
package edu.virginia.lib.ld2solr;

import static com.google.common.collect.Maps.transformEntries;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collection;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps.EntryTransformer;

import edu.virginia.lib.ld2solr.api.NamedFields;

/**
 * Exports a {@link NamedFields} instance as a {@link SolrDocument}.
 * 
 * @author ajs6f
 * 
 */
public class WrappedNamedFields implements Supplier<SolrInputDocument> {

	private final NamedFields fields;

	// TODO make index-time boost somehow adjustable, or something
	public static final Long INDEX_TIME_BOOST = 1L;

	private static final Logger log = getLogger(WrappedNamedFields.class);

	@Override
	public SolrInputDocument get() {
		log.trace("Constructing new SolrInputDocument...");
		return new SolrInputDocument(transformEntries(fields, collection2solrInputField));
	}

	/**
	 * @param namedFields
	 *            the fields to wrap for export
	 */
	public WrappedNamedFields(final NamedFields namedFields) {
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

}
