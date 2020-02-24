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

    public void addTuple(Tuple<String, String> tuple) {
        this.tuples.add(tuple);
    }
}
