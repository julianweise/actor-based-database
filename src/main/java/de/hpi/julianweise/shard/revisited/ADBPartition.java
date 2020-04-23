package de.hpi.julianweise.shard.revisited;

import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.ADBEntity;

import java.util.List;
import java.util.Map;


public class ADBPartition extends AbstractBehavior<ADBPartition.Command> {

    public static int MAX_SIZE_BYTE = 4000;

    private final List<ADBEntity> data;
    private final ADBPartitionHeader header;
    private final Map<String, ADBSortedEntityAttributes2> sortedAttributes;

    public interface Command {}

    public ADBPartition(ActorContext<Command> context, List<ADBEntity> data) {
        super(context);
        assert data.size() > 0 && data.stream().mapToInt(ADBEntity::getSize).sum() < MAX_SIZE_BYTE;

        this.data = data;
        this.header = ADBPartitionHeaderFactory.createDefault(data);
        this.sortedAttributes = ADBSortedEntityAttributes2Factory.of(data);
    }

    @Override
    public Receive<Command> createReceive() {
        return null;
    }
}
