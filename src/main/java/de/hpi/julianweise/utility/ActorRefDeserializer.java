package de.hpi.julianweise.utility;

import akka.actor.ExtendedActorSystem;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorRefResolver;
import akka.actor.typed.javadsl.Adapter;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

public class ActorRefDeserializer<T> extends JsonDeserializer<ActorRef<T>> {

    private final ActorRefResolver actorRefResolver;

    public ActorRefDeserializer(ExtendedActorSystem extendedActorSystem) {
        this.actorRefResolver = ActorRefResolver.get(Adapter.toTyped(extendedActorSystem));
    }

    @Override
    public ActorRef<T> deserialize(JsonParser jsonParser, DeserializationContext d) throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return actorRefResolver.resolveActorRef(node.asText());
    }
}
