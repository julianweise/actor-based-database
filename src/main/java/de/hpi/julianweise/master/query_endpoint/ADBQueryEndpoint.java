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
import de.hpi.julianweise.query.ADBQuery;
import lombok.AllArgsConstructor;
import lombok.Getter;
import scala.concurrent.duration.Duration;

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
        return Directives.concat(
                Directives.path("query",
                () -> Directives.withoutRequestTimeout(
                        () -> Directives.post(() -> Directives.entity(
                        Jackson.unmarshaller(ADBQuery.class), this::handleSyncQuery)))),
                Directives.path("query-async",
                        () -> Directives.withRequestTimeout(Duration.fromNanos(2e9),
                                () -> Directives.post(() -> Directives.entity(
                                Jackson.unmarshaller(ADBQuery.class), this::handleAsyncQuery))))
        );
    }

    private RouteAdapter handleAsyncQuery(ADBQuery query) {
        return this.handleQuery(query, true);
    }

    private RouteAdapter handleSyncQuery(ADBQuery query) {
        return this.handleQuery(query, false);
    }

    private RouteAdapter handleQuery(ADBQuery query, boolean async) {
        this.getContext().getLog().info("Received new query: " + query);
        CompletableFuture<Object[]> future = new CompletableFuture<>();
        int requestId = this.requestCounter.getAndIncrement();
        this.requests.put(requestId, future);
        this.shardInquirer.tell(ADBPartitionInquirer.QueryShards.builder()
                                                                .query(query)
                                                                .requestId(requestId)
                                                                .async(async)
                                                                .respondTo(this.shardInquirerResponseWrapper)
                                                                .build());
        return Directives.onSuccess(future,
                extracted -> Directives.complete(this.visualize(extracted)));
    }

    private String visualize(Object[] results) {
        StringBuilder builder = new StringBuilder();
        for(Object element : results) {
            builder.append(element.toString());
            builder.append("\n");
        }
        return builder.toString();
    }

    private Behavior<Command> handlePostStop(PostStop signal) {
        this.binding.thenCompose(ServerBinding::unbind);
        return Behaviors.same();
    }

    private Behavior<Command> handleShardInquirerResponseWrapper(ShardInquirerResponseWrapper wrapper) {
        if (wrapper.getResponse() instanceof ADBPartitionInquirer.SyncQueryResults) {
            ADBPartitionInquirer.SyncQueryResults res = (ADBPartitionInquirer.SyncQueryResults) wrapper.getResponse();
            this.requests.get(res.getRequestId()).complete(res.getResults());
        } else if (wrapper.getResponse() instanceof ADBPartitionInquirer.AsyncQueryResults) {
            ADBPartitionInquirer.AsyncQueryResults res = (ADBPartitionInquirer.AsyncQueryResults) wrapper.getResponse();
            Integer[] transactionId = {res.getTransactionId()};
            this.requests.get(res.getRequestId()).complete(transactionId);
        }
        return Behaviors.same();
    }

}