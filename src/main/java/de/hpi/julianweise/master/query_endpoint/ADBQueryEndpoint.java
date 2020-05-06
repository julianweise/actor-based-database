package de.hpi.julianweise.master.query_endpoint;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.server.Directives;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.directives.RouteAdapter;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import de.hpi.julianweise.domain.custom.SFEmployeeSalary;
import de.hpi.julianweise.query.ADBPartitionInquirer;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBQueryEndpoint extends AbstractBehavior<ADBQueryEndpoint.Command> {

    private final ActorRef<ADBPartitionInquirer.Command> shardInquirer;
    private final ActorRef<ADBPartitionInquirer.Response> shardInquirerResponseWrapper;
    private final Map<Integer, CompletableFuture<Object[]>> requests = new HashMap<>();
    private final AtomicInteger requestCounter = new AtomicInteger();
    private CompletionStage<ServerBinding> binding;

    public interface Command {
    }

    @AllArgsConstructor
    @Getter
    public static class ShardInquirerResponseWrapper implements Command {
        private final ADBPartitionInquirer.Response response;

    }

    public ADBQueryEndpoint(ActorContext<Command> context, String hostname, int port,
                            ActorRef<ADBPartitionInquirer.Command> shardInquirer,
                            ActorRef<ADBPartitionInquirer.Response> shardInquirerResponseWrapper) {
        super(context);
        this.shardInquirer = shardInquirer;
        this.shardInquirerResponseWrapper = shardInquirerResponseWrapper;
        this.initializeHTTPEndpoint(hostname, port);
    }

    private void initializeHTTPEndpoint(String hostname, int port) {
        Http http = Http.get(this.getContext().getSystem().classicSystem());
        Materializer materializer = Materializer.createMaterializer(this.getContext().getSystem());

        Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = this.createRoute().flow(this.getContext().getSystem().classicSystem(),
                materializer);
        this.binding = http.bindAndHandle(routeFlow, ConnectHttp.toHost(hostname, port), materializer);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, this::handlePostStop)
                .onMessage(ShardInquirerResponseWrapper.class, this::handleShardInquirerResponseWrapper)
                .build();
    }

    private Route createRoute() {
        return Directives.concat(Directives.path("query",
                () -> Directives.withoutRequestTimeout(() -> Directives.post(() -> Directives.entity(
                        Jackson.unmarshaller(ADBQuery.class), this::handleQuery))))
        );
    }

    private RouteAdapter handleQuery(ADBQuery query) {
        this.getContext().getLog().info("Received new query: " + query);
        CompletableFuture<Object[]> future = new CompletableFuture<>();
        int requestId = this.requestCounter.getAndIncrement();
        this.requests.put(requestId, future);
        this.shardInquirer.tell(ADBPartitionInquirer.QueryShards.builder()
                                                                .query(query)
                                                                .requestId(requestId)
                                                                .respondTo(this.shardInquirerResponseWrapper)
                                                                .build());
        return Directives.onSuccess(future,
                extracted -> Directives.complete(this.printListOfEmployeeIdentifiers(extracted)));
    }

    @SuppressWarnings("unchecked")
    private String printListOfEmployeeIdentifiers(Object[] extracted) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object o : extracted) {
            ADBPair<SFEmployeeSalary, SFEmployeeSalary> tuple = (ADBPair<SFEmployeeSalary, SFEmployeeSalary>) o;
            stringBuilder.append(tuple.getKey().getEmployeeIdentifier()).append(", ").append(tuple.getValue().getEmployeeIdentifier()).append("\n");
        }
        return stringBuilder.toString();
    }

    private Behavior<Command> handlePostStop(PostStop signal) {
        this.binding.thenCompose(ServerBinding::unbind);
        return Behaviors.same();
    }

    private Behavior<Command> handleShardInquirerResponseWrapper(ShardInquirerResponseWrapper wrapper) {
        if (wrapper.getResponse() instanceof ADBPartitionInquirer.AllQueryResults) {
            ADBPartitionInquirer.AllQueryResults response = (ADBPartitionInquirer.AllQueryResults) wrapper.getResponse();
            this.requests.get(response.getRequestId()).complete(response.getResults());
        }
        return Behaviors.same();
    }

}
