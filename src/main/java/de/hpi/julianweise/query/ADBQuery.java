package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import it.unimi.dsi.fastutil.objects.ObjectList;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME;

@JsonTypeInfo(use = NAME, include = PROPERTY)
@JsonSubTypes({
                      @JsonSubTypes.Type(value=ADBSelectionQuery.class, name = "ADBSelectionQuery"),
                      @JsonSubTypes.Type(value=ADBJoinQuery.class, name = "ADBJoinQuery")
              })
public interface ADBQuery {

    ObjectList<? extends ADBQueryTerm> getTerms();

}
