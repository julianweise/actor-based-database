package de.hpi.julianweise.query.session.select;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

public class ADBSelectQuerySession extends ADBQuerySession {

    private final List<ADBEntity> queryResults = new ArrayList<>();

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @SuperBuilder
    public static class SelectQueryResults extends ADBQuerySession.QueryResults {
        private List<ADBEntity> results;

    }

    public ADBSelectQuerySession(ActorContext<Command> context, List<ActorRef<ADBShard.Command>> shards,
                                 int transactionId, ActorRef<ADBShardInquirer.Command> parent, ADBSelectionQuery query) {
        super(context, shards, transactionId, parent);
        // Send initial query
        this.distributeQuery(query);
    }

    private void distributeQuery(ADBQuery query) {
        for(ActorRef<ADBShard.Command> shard : this.shards) {
            shard.tell(ADBShard.QueryEntities.builder()
                                             .transactionId(transactionId)
                                             .clientLargeMessageReceiver(this.initializeTransferWrapper)
                                             .query(query)
                                             .respondTo(this.getContext().getSelf())
                                             .build());
            this.expectedPartialResults.incrementAndGet();
        }
    }

    @Override
    public Receive<Command> createReceive() {
        return createReceiveBuilder()
                .onMessage(SelectQueryResults.class, this::handleQueryResults)
                .build();
    }

    private Behavior<ADBQuerySession.Command> handleQueryResults(SelectQueryResults response) {
        this.expectedPartialResults.decrementAndGet();
        this.queryResults.addAll(response.getResults());
        this.conditionallyConcludeTransaction();
        return Behaviors.same();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }

    @Override
    protected void submitResults() {
        this.parent.tell(new ADBShardInquirer.TransactionResults(this.transactionId, this.queryResults.toArray()));
    }
}
