package de.hpi.julianweise.master.query.select;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.master.query.ADBMasterQuerySession;
import de.hpi.julianweise.master.query_endpoint.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.largemessage.ADBLargeMessageReceiver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.val;

import java.util.Collections;
import java.util.List;

public class ADBMasterSelectSession extends ADBMasterQuerySession {

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @SuperBuilder
    public static class SelectQueryResults extends ADBMasterQuerySession.QueryResults {
        private List<ADBEntity> results;

    }

    public ADBMasterSelectSession(ActorContext<Command> context, List<ActorRef<ADBQueryManager.Command>> queryManager,
                                  int transactionId, ActorRef<ADBPartitionInquirer.Command> parent, ADBSelectionQuery query) {
        super(context, queryManager, transactionId, parent);
        // Send initial query
        this.distributeQuery(query);
    }

    private void distributeQuery(ADBQuery query) {
        val respondTo = this.getContext().messageAdapter(ADBLargeMessageReceiver.InitializeTransfer.class,
                InitializeTransferWrapper::new);
        for (ActorRef<ADBQueryManager.Command> manager : this.queryManagers) {
            manager.tell(ADBQueryManager.QueryEntities
                    .builder()
                    .transactionId(transactionId)
                    .clientLargeMessageReceiver(respondTo)
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
        this.parent.tell(new ADBPartitionInquirer.TransactionResultChunk(this.transactionId, response.results, false));
        this.conditionallyConcludeTransaction();
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }

    @Override
    protected void submitResults() {
        this.parent.tell(new ADBPartitionInquirer.TransactionResultChunk(this.transactionId, Collections.emptyList(), true));
    }
}
