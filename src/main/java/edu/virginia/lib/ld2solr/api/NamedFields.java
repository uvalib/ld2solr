package edu.virginia.lib.ld2solr.api;

import static com.google.common.collect.Collections2.transform;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;

/**
 * A {@link Map} of named fields. Any field may be multivalued.
 * 
 * @author ajs6f
 * 
 */
public class NamedFields extends HashMap<String, Collection<String>> {

	private static final long serialVersionUID = 1L;

	public NamedFields(final Map<String, Collection<?>> wrappedFields) {
		this.putAll(Maps.transformEntries(wrappedFields, asEntryOfStringToCollectionOfStrings));
	}

	private static final Function<Collection<?>, Collection<String>> asCollectionOfStrings = new Function<Collection<?>, Collection<String>>() {

		@Override
		public Collection<String> apply(final Collection<?> input) {
			return transform(input, asString);
		}
	};

	private static final EntryTransformer<String, Collection<?>, Collection<String>> asEntryOfStringToCollectionOfStrings = new EntryTransformer<String, Collection<?>, Collection<String>>() {

		@Override
		public Collection<String> transformEntry(final String key, final Collection<?> value) {

			return asCollectionOfStrings.apply(value);
		}
	};
	private static final Function<Object, String> asString = new Function<Object, String>() {

		@Override
		public String apply(final Object input) {
			return input.toString();
		}
	};

}
