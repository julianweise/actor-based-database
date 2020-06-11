package de.hpi.julianweise.master.query_endpoint;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.master.io.ADBResultWriter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicInteger;

@AllArgsConstructor
@Builder
@Setter
@Getter
public class ADBTransactionContext {
    int transactionId;
    int requestId;
    ADBPartitionInquirer.QueryNodes initialRequest;
    long startTime;
    long duration;
    ActorRef<ADBResultWriter.Command> resultWriter;
    ActorRef<ADBPartitionInquirer.QueryConclusion> respondTo;
    final AtomicInteger resultsCount = new AtomicInteger(0);
}
