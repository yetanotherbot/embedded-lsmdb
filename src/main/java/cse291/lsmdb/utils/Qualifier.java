package cse291.lsmdb.utils;

/**
 * Created by CielBlade on 6/10/17.
 */
public class Qualifier {
    private String operator;
    private String target;
    public Qualifier(){}
    public Qualifier(String operator, String target){
        this.operator = operator;
        this.target = target;
    }

    public Boolean qualify(String s){
        if(operator == null){
            return true;
        }
        switch(operator){
            case "<":
                return s.compareTo(target) == -1;
            case ">":
                return s.compareTo(target) == 1;
            case ">=":
                return s.compareTo(target) >= 0;
            case "<=":
                return s.compareTo(target) <= 0;
            case "=":
                return s.compareTo(target) == 0;
            default:
                return false;
        }
    }
}
