package it.albertotn.mongodb.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.mongodb.util.JSON;

/**
 * Utility class for handling {@link Document}
 */
public final class DocumentUtil {

    private static final Logger log = LoggerFactory.getLogger(DocumentUtil.class);	

	private static final Object ID = "_id";

	private DocumentUtil() {
	}

	/**
	 * Convert a list of {@link Document} to a list of {@link String} containing
	 * corresponding json
	 * 
	 * @param result
	 * @return
	 */
	public static List<String> convertToString(final List<Document> result) {
		if (result == null) {
			return new ArrayList<>();
		}
		List<String> res = new ArrayList<>();
		result.stream().forEachOrdered(r -> res.add(r.toJson()));
		return res;
	}

	/**
	 * @return from a list of {@link Document} correspondi list of {@link ObjectId}
	 */
	public static List<ObjectId> getDocumentIds(final List<Document> mainDocuments) {
		if (mainDocuments == null) {
			return new ArrayList<>();
		}
		return mainDocuments.stream().map(d -> d.getObjectId(ID)).collect(Collectors.toList());
	}

	/**
	 * Apply a jsonPath and extract value for that path
	 * 
	 * @param document
	 * @param jsonPath
	 * @return null if any or input null
	 */
	public static Object apply(final Document document, final String jsonPath) {
		return apply(document, jsonPath, false);
	}

	/**
	 * Apply a jsonPath and extract value for that path
	 * 
	 * @param document
	 * @param jsonPath
	 * @return null if any or input null
	 */
	public static Object apply(final Document document, final String jsonPath, final boolean printLog) {
		if (document == null || jsonPath == null) {
			return null;
		}
		final String json = JSON.serialize(document);
		try {
			return JsonPath.read(json, jsonPath);
		} catch (PathNotFoundException pnf) {
			if (printLog) {
				log.error("Error during jsonpath read", pnf);
			}
			return null;
		}
	}

	public static boolean isEmpty(Bson bson) {
		if (bson == null) {
			return true;
		}
		return ((Document) bson).toJson().equals("{ }");
	}

	/**
	 * @param document
	 * @return a new {@link Document} where all arrays are removed and first element
	 * of each of them are set
	 */
	public static Document unArray(final Document document) {
		if (document == null) {
			return null;
		}
		Document result = new Document();
		for (Entry<String, Object> entry : document.entrySet()) {
			if (isBase(entry.getValue())) {
				result.append(entry.getKey(), entry.getValue());
			} else if (entry.getValue() instanceof List) {
				List<?> val = (List<?>) entry.getValue();
				if (val != null && !val.isEmpty() && val.size() == 1 && isBase(val.get(0))) {
					result.append(entry.getKey(), val.get(0));
				} else {
					result.append(entry.getKey(), internalUnArray(val));
				}
			} else if (entry.getValue() instanceof Document) {
				result.append(entry.getKey(), unArray((Document) entry.getValue()));
			}

		}
		return result;
	}

	private static Document internalUnArray(List<?> values) {
		if (values == null) {
			return null;
		}
		Object current = values.get(0);
		if (current instanceof Document) {
			return unArray((Document) current);
		} else if (values.get(0) instanceof List) {
			return unArray(((List<Document>) current).get(0));
		}
		return new Document(values.get(0).toString(), values.get(1));
	}

	private static boolean isBase(Object value) {
		if (value == null) {
			return false;
		}
		return value instanceof String || value instanceof Long || value instanceof Integer
				|| value instanceof Date /* || value instanceof ObjectId */;
	}

	/**
	 * @param value
	 * @return a string without \\
	 */
	public static String escapeEscape(String value) {
		if (value == null || value.isEmpty()) {
			return value;
		}
		if (value.contains("\\")) {
			return value.replace("\\\\", "\\");
		} else if (value.contains("//")) {
			return value.replace("//", "/");
		}
		return value;
	}

	/**
	 * Given a path with dot notation (f.e. book.author) and a value create
	 * corresponding {@link Document} with nested fields<br>
	 * <br>
	 * path : book.author<br>
	 * value : Asimov<br>
	 * <br>
	 * result:<br>
	 * 
	 * <pre>
	 * { "book" : { "author" : "Asimov" } }
	 * </pre>
	 * 
	 * @param path
	 * @param value
	 * @return {@link Document} with requested structure or null if input is null
	 */
	public static Document nestedCreator(final String path, final Object value) {
		if (StringUtils.isEmpty(path)) {
			return null;
		}
		if (!path.contains(".")) {
			return new Document(path, value);
		}
		String[] paths = StringUtils.split(path, ".");
		return recursiveNestedCreator(paths, 0, value, new Document());
	}

	static Document recursiveNestedCreator(String[] paths, int i, Object value, Document current) {
		if (paths == null) {
			return current;
		}
		if (i < paths.length - 1) {
			current.append(paths[i], recursiveNestedCreator(paths, ++i, value, new Document()));
		} else if (i == paths.length - 1) {
			current.append(paths[i], value);
		}
		return current;
	}

	/**
	 * Union provided documents (no changes on input document, immutable) <br>
	 * 
	 * @return union Document
	 */
	public static Document union(final Document first, final Document second) {
		if (first == null || second == null) {
			return null;
		}
		Document result = new Document();
		if (first.isEmpty()) {
			return second;
		}
		if (second.isEmpty()) {
			return first;
		}
		// first see if there are matching keys between first and second
		for (Entry<String, Object> entry : first.entrySet()) {
			if (second.containsKey(entry.getKey())) {
				Object f = first.get(entry.getKey());
				Object s = second.get(entry.getKey());
				if (f instanceof Document && s instanceof Document) {
					Document unionDoc = union((Document) f, (Document) s);
					result.append(entry.getKey(), unionDoc);
				}
			} else {
				result.append(entry.getKey(), entry.getValue());
			}
		}
		for (Entry<String, Object> entry : second.entrySet()) {
			if (!result.containsKey(entry.getKey())) {
				result.append(entry.getKey(), entry.getValue());
			}
		}
		return result;
	}

	/**
	 * return a copy of given Document, but remove field, be aware that is a shallow
	 * copy, not a deep copy!
	 */
	public static Document copyRemoveFirstLevelField(final Document doc, final String field) {
		if (doc == null || field == null) {
			return null;
		}
		Document res = new Document();
		for (String key : doc.keySet()) {
			if (!key.equals(field)) {
				res.append(key, doc.get(key));
			}
		}
		return res;
	}

	/**
	 * For every attribute of this doc, if name of attribute use dot notation
	 * (a.b.c), nested create attributes
	 * 
	 * @param input document
	 * @return nested created document with no dot notation attribute
	 */
	public static Document nestedAttributeCreator(final Map<String, Object> doc) {
		if (doc == null) {
			return null;
		}
		if (doc.keySet().isEmpty()) {
			return new Document(doc);
		}
		Document res = new Document();
		for (String key : doc.keySet()) {
			if (doc.get(key) instanceof ArrayList) {
				List<?> list = (ArrayList<?>) doc.get(key);
				if (list.isEmpty()) {
					res = union(res, new Document(key, doc.get(key)));
				}
				Document innerDoc = new Document();
				for (Object elem : list) {
					if (elem instanceof Map) {
						innerDoc = union(res, nestedAttributeCreator((Map<String, Object>) elem));
					} else if (elem instanceof String) {
						// test only
					}
				}
				if (!innerDoc.isEmpty()) {
					ArrayList<Document> tmp = new ArrayList<>();
					tmp.add(innerDoc);
					res = union(res, new Document(key, tmp));
				}
			} else {
				if (!key.contains(".")) {
					res.append(key, doc.get(key));
				} else {
					Document tmp = nestedCreator(key, doc.get(key));
					String subKey = tmp.keySet().iterator().next();
					res = union(res, new Document(subKey, tmp.get(subKey)));
				}
			}
		}
		return res;
	}

	/**
	 * Find a sub document with couple attribute/value as string
	 * 
	 * @return null if nothing is found, otherwise subdocument
	 */
	public static Document findByAttribute(Document doc, String key, String value) {
		if (doc == null || key == null || value == null) {
			return null;
		}
		if (doc.containsKey(key)) {
			Object val = doc.get(key);
			if (val instanceof String && ((String) val).equals(value)) {
				return doc;
			}
		}
		Document res = null;
		for (String k : doc.keySet()) {
			Object val = doc.get(k);
			if (val instanceof Document) {
				res = findByAttribute((Document) val, key, value);
				if (res != null) {
					return res;
				}
			}
		}
		return null;
	}

}
