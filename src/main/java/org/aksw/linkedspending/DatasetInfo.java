package org.aksw.linkedspending;

import java.time.Instant;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DatasetInfo
{
	public final String name;
	public final Instant created;
	public final Instant modified;
}