package org.aksw.linkedspending;

import java.time.Instant;
import java.util.SortedMap;
import java.util.TreeMap;
import org.aksw.linkedspending.tools.DataModel;
import org.aksw.linkedspending.tools.PropertyLoader;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.vocabulary.DCTerms;

public class Sparql
{
	// TODO when datasets are newly transformed, remove this
	static final String WRONG_CREATED = "http://dublincore.org/documents/2012/06/14/dcmi-terms/created";
	public static final String PREFIXES = "prefix dcterms: <"+DCTerms.getURI()
			+">\n prefix ls: <"+PropertyLoader.prefixInstance
			+">\n prefix lso: <"+PropertyLoader.prefixOntology
			+">\n prefix qb: <"+DataModel.DataCube.base+">\n";

	public static ResultSet selectPrefixed(String query)
	{
		return select(PREFIXES+query);
	}

	public static ResultSet select(String query)
	{
		QueryEngineHTTP qe = new QueryEngineHTTP(PropertyLoader.endpoint, query);
		return qe.execSelect();
	}

	public static SortedMap<String,Long> modifiedByName()
	{
		SortedMap<String,Long> datasets = new TreeMap<>();
		SortedMap<Long,String> datasetsByTime = datasetsByModified();
		for(Long time: datasetsByModified().keySet()) datasets.put(datasetsByTime.get(time), time);
		return datasets;
	}

	public static SortedMap<Long,String> datasetsByModified()
	{
		// TODO when datasets are newly transformed, just use dcterms:identifier
		SortedMap<Long,String> datasets = new TreeMap<>();
		String query = "select ?d ?m {?d a qb:DataSet. ?d <"+DCTerms.modified.getURI()+"> ?m. ?d <"+DCTerms.created.getURI()+"> ?c.}";
		ResultSet rs = select(query);
		QuerySolution qs;
		while(rs.hasNext())
		{
			qs=rs.next();
			datasets.put(Instant.parse(qs.get("c").asLiteral().getLexicalForm()).toEpochMilli(), qs.get("d").asResource().getURI().replace("http://linkedspending.aksw.org/instance/", ""));
		}
		return datasets;
	}
}