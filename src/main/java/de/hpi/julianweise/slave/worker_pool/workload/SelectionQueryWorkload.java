package de.hpi.julianweise.slave.worker_pool.workload;

import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.slave.partition.column.pax.ADBColumn;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.worker_pool.GenericWorker;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

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
        ObjectArrayList<ADBEntity> results = new ObjectArrayList<>();
        int guardedSize = this.columns.values().stream().mapToInt(ADBColumn::size).min().orElse(0);
        for (int i = 0; i < guardedSize; i++) {
            if (!this.rowSatisfyQuery(i)) {
                continue;
            }
            results.add(this.reassemble(i));
        }
        message.getRespondTo().tell(new Results(results));
    }

    private boolean rowSatisfyQuery(int index) {
        for (ADBSelectionQueryPredicate predicate : this.query.getPredicates()) {
            if (this.columns.get(predicate.getFieldName()).satisfy(index, predicate.getValue())) {
                return false;
            }
        }
        return true;
    }

    @SneakyThrows
    private ADBEntity reassemble(int index) {
        ADBEntity entity = ADBEntityFactoryProvider.getInstance().getTargetClass().newInstance();
        for (ADBColumn column : this.columns.values()) {
            column.setField(entity, index);
        }
        return entity;
    }
}