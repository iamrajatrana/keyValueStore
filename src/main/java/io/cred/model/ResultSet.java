package io.cred.model;

import java.util.ArrayList;
import java.util.List;

public class ResultSet {

    List<Row> rows = new ArrayList<>();

    public List<Row> getRows() {
        return rows;
    }

    public void addRow(Row row) {
        rows.add(row);
    }

    @Override
    public String toString() {
        return "Result :: " + rows;
    }
}
