package de.hpi.julianweise.domain.custom.patient;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;

@SuppressWarnings({"SpellCheckingInspection", "unsed"})
public class PatientDeserializer extends JsonDeserializer<Patient> {

    @Override
    public Patient deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        return Patient.builder()
                      .ausgleichsjahr(node.get("ausgleichsjahr").asInt())
                      .berichtsjahr(node.get("berichtsjahr").asInt())
                      .psid2(node.get("psid2").asInt())
                      .psid(node.get("psid").asText())
                      .kvNrKennzeichen(node.get("kvNrKennzeichen").asBoolean())
                      .geburtsjahr(node.get("geburtsjahr").asInt())
                      .geschlecht(node.get("geschlecht").asText().charAt(0))
                      .versichertenTage(node.get("versichertenTage").asInt())
                      .verstorben(node.get("verstorben").asBoolean())
                      .versichertentageKrankenGeld(node.get("versichertentageKrankenGeld").asInt())
                      .build();
    }
}
