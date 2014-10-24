package org.aksw.linkedspending.rest;

import java.time.Instant;
import java.util.SortedMap;
import java.util.TreeMap;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.vocabulary.DCTerms;

public class Sparql
{
	public final static String endpoint = "http://linkedspending.aksw.org/sparql";
	// TODO when datasets are newly transformed, remove this
	static final String WRONG_CREATED = "http://dublincore.org/documents/2012/06/14/dcmi-terms/created";

	public static ResultSet select(String query)
	{
		QueryEngineHTTP qe = new QueryEngineHTTP(endpoint, query);
		return qe.execSelect();
	}

	public static SortedMap<String,Long> datasetsByName()
	{
		SortedMap<String,Long> datasets = new TreeMap<>();
		SortedMap<Long,String> datasetsByTime = datasetsByTime();
		for(Long time: datasetsByTime().keySet()) datasets.put(datasetsByTime.get(time), time);
		return datasets;
	}

	public static SortedMap<Long,String> datasetsByTime()
	{
		// TODO when datasets are newly transformed, just use dcterms:identifier
		SortedMap<Long,String> datasets = new TreeMap<>();
		String query = "select ?d ?c {?d a qb:DataSet. {?d <"+WRONG_CREATED+"> ?c.} UNION {?d <"+DCTerms.created.getURI()+"> ?c.}}";
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