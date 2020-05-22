package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME;

@JsonTypeInfo(use = NAME, include = PROPERTY)
@JsonSubTypes({
                      @JsonSubTypes.Type(value=ADBSelectionQuery.class, name = "ADBSelectionQuery"),
                      @JsonSubTypes.Type(value=ADBJoinQuery.class, name = "ADBJoinQuery")
              })
public interface ADBQuery {
    List<? extends ADBQueryTerm> getTerms();
}
