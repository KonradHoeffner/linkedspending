package org.aksw.linkedspending;

import static org.junit.Assert.*;
import java.time.Instant;
import org.aksw.linkedspending.exception.DataSetDoesNotExistException;
import org.junit.Test;

public class OpenSpendingDatasetInfoTest
{

	@Test public void testOsDatasetInfo() throws DataSetDoesNotExistException
	{
		assertTrue(OpenSpendingDatasetInfo.forDataset("2013").created.isBefore(Instant.now()));
	}
}