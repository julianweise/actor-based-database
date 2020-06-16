package de.hpi.julianweise.master.data_loading;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import org.apache.commons.csv.CSVRecord;

public class ADBCSVToEntityConverter extends AbstractBehavior<ADBCSVToEntityConverter.Command> {

    public interface Command {}

    public interface Response {}

    @AllArgsConstructor
    public static class ConvertBatch implements Command {
        ActorRef<ConvertedBatch> respondTo;
        ObjectList<CSVRecord> batch;
    }

    @AllArgsConstructor
    public static class ConvertedBatch  implements Response {
        ObjectList<ADBEntity> entities;
    }

    public static Behavior<Command> createDefault() {
        return Behaviors.setup(ADBCSVToEntityConverter::new);
    }

    public ADBCSVToEntityConverter(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ConvertBatch.class, this::handleBatch)
                .build();
    }

    private Behavior<Command> handleBatch(ConvertBatch command) {
        ObjectList<ADBEntity> converted = new ObjectArrayList<>(command.batch.size());
        for(CSVRecord record : command.batch) {
            converted.add(ADBEntityFactoryProvider.getInstance().build(record));
        }
        command.respondTo.tell(new ConvertedBatch(converted));
        return Behaviors.same();
    }
}
