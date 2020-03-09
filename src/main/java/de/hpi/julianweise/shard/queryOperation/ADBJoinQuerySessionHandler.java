package de.hpi.julianweise.shard.queryOperation;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.Signal;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.shard.ADBShard;
import javafx.util.Pair;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ADBJoinQuerySessionHandler extends ADBQuerySessionHandler {

    private Map<String, int[]> joinAttributes = new HashMap<>();

    public ADBJoinQuerySessionHandler(ActorContext<Command> context,
                                      ActorRef<ADBShard.Command> shard,
                                      ActorRef<ADBQuerySession.Command> client, int transactionId,
                                      ADBJoinQuery query, final List<ADBEntityType> data) {
        super(context, shard, client, transactionId, query, data);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, this::handlePostStop)
                .onMessage(Execute.class, this::handleExecute)
                .build();
    }

    private Behavior<Command> handlePostStop(Signal postStop) {
        this.joinAttributes = null;
        System.gc();
        return Behaviors.same();
    }

    private Behavior<Command> handleExecute(Execute command) {
        this.extractSortedJoinAttributes();
        return this.concludeTransaction();
    }

    private void extractSortedJoinAttributes() {
        for (ADBQueryTerm term : this.query.getTerms()) {
            ADBJoinQueryTerm joinTerm = (ADBJoinQueryTerm) term;
            this.extractSortedJoinAttributesForField(joinTerm.getFieldName());
        }
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private void extractSortedJoinAttributesForField(String fieldName) {
        if (this.joinAttributes.containsKey(fieldName)) {
            return;
        }
        Field joinAttribute = ADBEntityFactoryProvider.getInstance().getTargetClass().getDeclaredField(fieldName);
        List<Pair<Integer, Comparable<Object>>> fieldJoinAttributes = new ArrayList<>(this.data.size());
        for(int i = 0; i < this.data.size(); i++) {
            fieldJoinAttributes.add(new Pair<>(i, (Comparable<Object>) joinAttribute.get(this.data.get(i))));
        }
        fieldJoinAttributes.sort(Comparator.comparing(Pair::getValue));
        int[] indices = fieldJoinAttributes.stream().map(Pair::getKey).mapToInt(Integer::intValue).toArray();
        this.joinAttributes.put(fieldName, indices);
    }

    @Override
    protected String getQuerySessionName() {
        return "Join Query";
    }
}
