package cse291.lsmdb.utils;

import java.util.Set;

/**
 * Created by CielBlade on 6/10/17.
 */
public class Qualifier {
    private String operator;
    private String target;
    private Set<String> rowKeys;

    public Qualifier() {
    }

    public Qualifier(String operator, String target) {
        this.operator = operator;
        this.target = target;
    }

    public Qualifier(Set<String> rowKeys) {
        this.rowKeys = rowKeys;
    }

    public Boolean qualify(String rowName, String colValue) {
        if(operator == null && rowKeys == null){
            return true;
        }
        if(rowKeys != null){
            return rowKeys.contains(rowName);
        }
        switch (operator) {
            case "<":
                return colValue.compareTo(target) == -1;
            case ">":
                return colValue.compareTo(target) == 1;
            case ">=":
                return colValue.compareTo(target) >= 0;
            case "<=":
                return colValue.compareTo(target) <= 0;
            case "=":
                return colValue.compareTo(target) == 0;
            default:
                return false;
        }
    }
}
