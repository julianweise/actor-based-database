package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.select.ADBSelectQuerySession;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.shard.ADBShard;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ADBSelectQuerySessionHandler extends ADBQuerySessionHandler {

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());


    public ADBSelectQuerySessionHandler(ActorContext<ADBQuerySessionHandler.Command> context,
                                        ActorRef<ADBShard.Command> shard,
                                        ActorRef<ADBQuerySession.Command> client, int transactionId,
                                        ADBSelectionQuery query, final List<ADBEntityType> data,
                                        int globalShardId) {
        super(context, shard, client, transactionId, query, data, globalShardId);
    }

    @Override
    public Receive<ADBQuerySessionHandler.Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Execute.class, this::handleExecute)
                .build();
    }

    private Behavior<ADBQuerySessionHandler.Command> handleExecute(Execute command) {
        final AtomicInteger counter = new AtomicInteger();
        Collection<List<ADBEntityType>> results = this.data.stream()
                                                           .filter(entity -> entity.matches((ADBSelectionQuery) this.query))
                                                           .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / this.settings.QUERY_RESPONSE_CHUNK_SIZE))
                                                           .values();

        results.forEach(chunk -> this.client.tell(ADBSelectQuerySession.SelectQueryResults.builder()
                                                                                          .results(chunk)
                                                                                          .globalShardId(this.globalShardId)
                                                                                          .transactionId(transactionId)
                                                                                          .build()));
        this.concludeTransaction();
        return Behaviors.stopped();
    }

    @Override
    protected String getQuerySessionName() {
        return "Select Query";
    }

}
