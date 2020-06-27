package de.hpi.julianweise.slave.query.join.steps;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.ADBJoinPredicateCostModel;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.util.Map;

public class ADBColumnJoinStepExecutorFactory {

    public static Behavior<ADBColumnJoinStepExecutor.Command> createDefault(
            Map<String, ObjectList<ADBEntityEntry>> left,
            Map<String, ObjectList<ADBEntityEntry>> right,
            Object2IntMap<String> leftOriginalSizes,
            Object2IntMap<String> rightOriginalSizes,
            ObjectList<ADBJoinPredicateCostModel> costModels,
            ActorRef<ADBColumnJoinStepExecutor.StepExecuted> respondTo
    ) {
        return Behaviors.setup(context -> new ADBColumnJoinStepExecutor(context, left ,right, leftOriginalSizes,
                rightOriginalSizes,
                costModels,
                respondTo));
    }
}
