package de.hpi.julianweise.shard;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.ServiceKey;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBKey;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.settings.Settings;
import de.hpi.julianweise.settings.SettingsImpl;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ADBShard extends AbstractBehavior<ADBShard.Command> {

    public interface Command extends CborSerializable {
    }

    @Getter
    @AllArgsConstructor
    public static class PersistEntity implements Command {
        private ActorRef<ADBShardDistributor.Command> respondTo;
        private ADBEntityType entity;
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class QueryEntities implements Command {
        int transactionId;
        private ActorRef<ADBShardInquirer.Command> respondTo;
        private ADBQuery query;
    }

    public static ServiceKey<ADBShard.Command> SERVICE_KEY = ServiceKey.create(ADBShard.Command.class, "data-shard");

    private final SettingsImpl settings = Settings.SettingsProvider.get(getContext().getSystem());
    private final Map<ADBKey, ADBEntityType> data = new HashMap<>();


    protected ADBShard(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(PersistEntity.class, this::handleEntity)
                .onMessage(QueryEntities.class, this::handleQueryEntities)
                .build();
    }

    private Behavior<Command> handleEntity(PersistEntity command) {
        this.data.put(command.getEntity().getPrimaryKey(), command.getEntity());
        command.respondTo.tell(new ADBShardDistributor.ConfirmEntityPersisted(command.getEntity().getPrimaryKey()));
        return Behaviors.same();
    }

    private Behavior<Command> handleQueryEntities(QueryEntities command) {
        int transactionId = command.getTransactionId();
        final AtomicInteger counter = new AtomicInteger();
        Collection<List<ADBEntityType>> results = this.data.values().stream()
                                                           .filter(entity -> entity.matches(command.getQuery()))
                                                           .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / this.settings.QUERY_RESPONSE_CHUNK_SIZE))
                                                           .values();

        results.forEach(chunk -> command.getRespondTo().tell(new ADBShardInquirer.QueryResults(transactionId, chunk)));
        command.getRespondTo().tell(new ADBShardInquirer.ConcludeTransaction(transactionId));
        return Behaviors.same();
    }
}
