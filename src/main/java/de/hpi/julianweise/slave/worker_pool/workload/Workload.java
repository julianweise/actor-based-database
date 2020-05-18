package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.slave.worker_pool.GenericWorker;

public abstract class Workload {

    public final void execute(GenericWorker.WorkloadMessage message) {
        this.doExecute(message);
    }

    protected abstract void doExecute(GenericWorker.WorkloadMessage message);
}