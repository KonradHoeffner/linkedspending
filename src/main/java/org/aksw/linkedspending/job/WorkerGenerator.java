package org.aksw.linkedspending.job;

import de.konradhoeffner.commons.TriFunction;

public interface WorkerGenerator extends TriFunction<String, Job, Boolean, Worker> {}