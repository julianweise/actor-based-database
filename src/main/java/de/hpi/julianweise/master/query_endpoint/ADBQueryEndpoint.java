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
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Directives;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.directives.RouteAdapter;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.hpi.julianweise.query.ADBQuery;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class ADBQueryEndpoint extends AbstractBehavior<ADBQueryEndpoint.Command> {

    private final ActorRef<ADBPartitionInquirer.Command> shardInquirer;
    private final ActorRef<ADBPartitionInquirer.QueryConclusion> shardInquirerResponseWrapper;
    private final Int2ObjectMap<CompletableFuture<Object>> requests = new Int2ObjectOpenHashMap<>();
    private final AtomicInteger requestCounter = new AtomicInteger();
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private CompletionStage<ServerBinding> binding;

    public interface Command {
    }

    @AllArgsConstructor
    @Getter
    public static class ShardInquirerResponseWrapper implements Command {
        private final ADBPartitionInquirer.QueryConclusion response;

    }

    public ADBQueryEndpoint(ActorContext<Command> context, String hostname, int port,
                            ActorRef<ADBPartitionInquirer.Command> shardInquirer,
                            ActorRef<ADBPartitionInquirer.QueryConclusion> shardInquirerResponseWrapper) {
        super(context);
        this.shardInquirer = shardInquirer;
        this.shardInquirerResponseWrapper = shardInquirerResponseWrapper;
        this.initializeHTTPEndpoint(hostname, port);
    }

    private void initializeHTTPEndpoint(String hostname, int port) {
        Http http = Http.get(this.getContext().getSystem().classicSystem());
        Materializer mat = Materializer.createMaterializer(this.getContext().getSystem());

        Flow<HttpRequest, HttpResponse, NotUsed> routeFlow = createRoute()
                .flow(getContext().getSystem().classicSystem(), mat);
        this.binding = http.bindAndHandle(routeFlow, ConnectHttp.toHost(hostname, port), mat);
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
                                () -> Directives.post(
                                        () -> Directives.entity(
                                                Jackson.unmarshaller(ADBQuery.class),
                                                query -> this.handleQuery((ADBQuery) query)))))
        );
    }

    private RouteAdapter handleQuery(ADBQuery query) {
        this.getContext().getLog().info("Received new query: " + query);
        CompletableFuture<Object> future = new CompletableFuture<>();
        int requestId = this.requestCounter.getAndIncrement();
        this.requests.put(requestId, future);
        this.shardInquirer.tell(ADBPartitionInquirer.QueryShards.builder()
                                                                .query(query)
                                                                .requestId(requestId)
                                                                .respondTo(this.shardInquirerResponseWrapper)
                                                                .build());
        return Directives.onSuccess(future, extracted -> Directives.complete(this.buildResponse(extracted)));
    }

    private HttpResponse buildResponse(Object content) {
        return HttpResponse.create()
                           .withStatus(StatusCodes.OK)
                           .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, this.mapToJson(content)));
    }

    private String mapToJson(Object results) {
        try {
            return this.jsonMapper.writeValueAsString(results);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "Unable to parse " + results.getClass() + " due to an JSON object mapper error";
        }
    }

    private Behavior<Command> handlePostStop(PostStop signal) {
        this.binding.thenCompose(ServerBinding::unbind);
        return Behaviors.same();
    }

    private Behavior<Command> handleShardInquirerResponseWrapper(ShardInquirerResponseWrapper wrapper) {
        this.requests.get(wrapper.response.getRequestId()).complete(ADBQueryEndpointResponse
                .builder()
                .duration(wrapper.response.getDuration())
                .transactionId(wrapper.response.getTransactionId())
                .resultsLocation(wrapper.response.getResultLocation())
                .numberOfResults(wrapper.response.getResultsCount())
                .build());
        return Behaviors.same();
    }

}