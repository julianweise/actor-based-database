package de.hpi.julianweise.master.query.select;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

public class ADBMasterSelectSession extends ADBMasterQuerySession {

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @SuperBuilder
    public static class SelectQueryResults extends ADBMasterQuerySession.QueryResults {
        private ObjectList<ADBEntity> results;

    }

    public ADBMasterSelectSession(ActorContext<Command> context,
                                  ObjectList<ActorRef<ADBQueryManager.Command>> queryManager,
                                  ObjectList<ActorRef<ADBPartitionManager.Command>> partitionManager,
                                  int transactionId,
                                  ActorRef<ADBPartitionInquirer.Command> parent,
                                  ADBSelectionQuery query) {
        super(context, queryManager, partitionManager, transactionId, parent);
        // Send initial query
        this.distributeQuery(query);
    }

    private void distributeQuery(ADBQuery query) {
        for (ActorRef<ADBQueryManager.Command> manager : this.queryManagers) {
            manager.tell(ADBQueryManager.QueryEntities
                    .builder()
                    .transactionId(transactionId)
                    .query(query)
                    .respondTo(this.getContext().getSelf())
                    .build());
        }
    }

    @Override
    public Receive<Command> createReceive() {
        return createReceiveBuilder()
                .onMessage(SelectQueryResults.class, this::handleQueryResults)
                .build();
    }

    private Behavior<ADBMasterQuerySession.Command> handleQueryResults(SelectQueryResults response) {
        this.parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, response.results,
                response.results.size(), false));
        this.conditionallyConcludeTransaction();
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }

    @Override
    protected void submitResults() {
        this.parent.tell(new ADBPartitionInquirer.TransactionResultChunk(transactionId, new ObjectArrayList<>(),
         0, true));
    }

    @Override
    protected boolean isFinalized() {
        return true;
    }
}
