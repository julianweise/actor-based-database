package de.hpi.julianweise.master.materialization;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import lombok.AllArgsConstructor;

public class ADBEntityMaterializer extends AbstractBehavior<ADBEntityMaterializer.Command> {

    public interface Command {}

    @AllArgsConstructor
    public static class MaterializeEntity implements Command {
        IntArrayList entityIds;
    }

    public ADBEntityMaterializer(ActorContext<Command> context) {
        super(context);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(MaterializeEntity.class, this::handleMaterializeEntity)
                .build();
    }

    private Behavior<Command> handleMaterializeEntity(MaterializeEntity command) {
        return Behaviors.same();
    }
}
