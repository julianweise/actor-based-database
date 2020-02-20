package de.hpi.julianweise.csv;

import de.hpi.julianweise.entity.Tuple;
import de.hpi.julianweise.utility.CborSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;


@Getter
@AllArgsConstructor
@NoArgsConstructor
public class CSVRow implements CborSerializable {

    public static String ID_IDENTIFIER = "_PSID";

    private List<Tuple<String, String>> tuples = new ArrayList<>();
    private String id = "";

    public void addTuple(Tuple<String, String> tuple) {
        if (tuple.getKey().contains(ID_IDENTIFIER)) {
            this.id = tuple.getValue();
        }

        this.tuples.add(tuple);
    }
}
