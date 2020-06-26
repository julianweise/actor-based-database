package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.slave.partition.column.pax.ADBColumn;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
public class SelectionQueryWorkload extends Workload {

    private final Map<String, ADBColumn> columns;
    private final ADBSelectionQuery query;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final ObjectList<ADBEntity> results;
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        // TODO
        message.getRespondTo().tell(new Results(new ObjectArrayList<>()));
    }

}
