package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class SelectionQueryWorkload extends Workload {

    private final ObjectList<ADBEntity> data;
    private final ADBSelectionQuery query;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final ObjectList<ADBEntity> results;
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        ObjectList<ADBEntity> results = this.data
                .stream()
                .filter(entity -> entity.matches(this.query))
                .collect(new ObjectArrayListCollector<>());
        message.getRespondTo().tell(new Results(results));
    }

}
