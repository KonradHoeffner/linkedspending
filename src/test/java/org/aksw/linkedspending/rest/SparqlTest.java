package org.aksw.linkedspending.rest;

import static org.junit.Assert.assertTrue;
import java.util.Map;
import org.aksw.linkedspending.Sparql;
import org.junit.Test;

public class SparqlTest
{

	@Test public void testDatasets()
	{
		Map<Long,String> datasets = Sparql.datasetsByTime();
		assertTrue(datasets.size()>500);
		assertTrue(datasets.size()<5000);
	}

}
