package tb.checksum.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PKUnion {
    private List<Object> pkColumns = new ArrayList<>();

    public void addKey(Object o){
        pkColumns.add(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PKUnion pkUnion = (PKUnion) o;
        return Objects.equals(pkColumns, pkUnion.pkColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pkColumns);
    }

    @Override
    public String toString() {
        return "PKUnion{" +
            "pkColumns=" + pkColumns +
            '}';
    }
}
