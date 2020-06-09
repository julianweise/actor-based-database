package de.hpi.julianweise.master.query_endpoint;

import de.hpi.julianweise.utility.serialization.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
public class ADBQueryEndpointResponse implements CborSerializable {
    private int transactionId;
    private long duration;
    private int numberOfResults;
    private String resultsLocation;
}
