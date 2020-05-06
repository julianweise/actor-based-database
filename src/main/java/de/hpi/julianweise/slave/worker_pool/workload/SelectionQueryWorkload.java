package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class SelectionQueryWorkload extends Workload {

    private final List<ADBEntity> data;
    private final ADBSelectionQuery query;

    @AllArgsConstructor
    @Getter
    public static class Results implements GenericWorker.Response {
        private final List<ADBEntity> results;
    }

    @Override
    public void doExecute(GenericWorker.WorkloadMessage message) {
        List<ADBEntity> results = this.data
                .stream()
                .filter(entity -> entity.matches(this.query))
                .collect(Collectors.toList());
        message.getRespondTo().tell(new Results(results));
    }

}
