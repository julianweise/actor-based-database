package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.benchmarking.ADBQueryPerformanceSampler;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;

public abstract class Workload {

    public final void execute(GenericWorker.WorkloadMessage message) {
        ADBQueryPerformanceSampler.log(true, this.getClass().getSimpleName(), "Compare attributes");
        this.doExecute(message);
        ADBQueryPerformanceSampler.log(false, this.getClass().getSimpleName(), "Compare attributes");
    }

    protected abstract void doExecute(GenericWorker.WorkloadMessage message);
}