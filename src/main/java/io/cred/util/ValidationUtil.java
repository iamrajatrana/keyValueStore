package io.cred.util;

import io.cred.exception.InvalidDatatypeException;
import io.cred.model.Attribute;
import io.cred.model.Entry;
import io.cred.model.Table;

import java.util.List;

public class ValidationUtil {
    public static boolean validate(Table table, List<Entry> entries) {
        for (Entry e : entries) {
            Attribute attribute = table.getAttributesMap().get(e.getName());
            if (attribute == null) continue;
            if (attribute.getDatatype() != e.getValue().getClass().getName())
                throw new InvalidDatatypeException("Invalid Datatype {} for " + e.getName());
        }
        return true;
    }
}
