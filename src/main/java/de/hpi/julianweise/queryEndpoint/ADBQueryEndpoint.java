package de.hpi.julianweise.queryEndpoint;

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
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBShardInquirer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBQueryEndpoint extends AbstractBehavior<ADBQueryEndpoint.Command> {

    public interface Command {
    }

    @AllArgsConstructor
    @Getter
    public static class ShardInquirerResponseWrapper implements Command {
        private ADBShardInquirer.Response response;
    }

    private CompletionStage<ServerBinding> binding;

    private final ActorRef<ADBShardInquirer.Command> shardInquirer;
    private final ActorRef<ADBShardInquirer.Response> shardInquirerResponseWrapper;
    private final Map<Integer, CompletableFuture<List<ADBEntityType>>> requests = new HashMap<>();
    private final AtomicInteger requestCounter = new AtomicInteger();

    public ADBQueryEndpoint(ActorContext<Command> context, String hostname, int port,
                            ActorRef<ADBShardInquirer.Command> shardInquirer,
                            ActorRef<ADBShardInquirer.Response> shardInquirerResponseWrapper) {
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
        return Directives.concat(Directives.path("query", () -> Directives.post(() -> Directives.entity(
                Jackson.unmarshaller(ADBSelectionQuery.class),
                query -> {
                    this.getContext().getLog().info("Received new query: " + query);
                    CompletableFuture<List<ADBEntityType>> future = new CompletableFuture<>();
                    int requestId = this.requestCounter.getAndIncrement();
                    this.requests.put(requestId, future);
                    this.shardInquirer.tell(ADBShardInquirer.QueryShards.builder()
                                                                        .query(query)
                                                                        .requestId(requestId)
                                                                        .respondTo(this.shardInquirerResponseWrapper)
                                                                        .build());
                    return Directives.onSuccess(future,
                            extracted -> Directives.complete(this.toJSON(extracted)));
                })))
        );
    }

    private Behavior<Command> handlePostStop(PostStop signal) {
        this.binding.thenCompose(ServerBinding::unbind);
        return Behaviors.same();
    }

    private Behavior<Command> handleShardInquirerResponseWrapper(ShardInquirerResponseWrapper wrapper) {
        if (wrapper.getResponse() instanceof ADBShardInquirer.AllQueryResults) {
            ADBShardInquirer.AllQueryResults response = (ADBShardInquirer.AllQueryResults) wrapper.getResponse();
            this.requests.get(response.getRequestId()).complete(response.getResults());
        }
        return Behaviors.same();
    }

    @SneakyThrows
    private String toJSON(List<ADBEntityType> results) {
        final ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(results);
    }

}
