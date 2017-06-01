package cse291.lsmdb.utils;

import java.util.Objects;

/**
 * Created by musteryu on 2017/5/30.
 */
public class Pair<Left, Right> {
    public final Left left;
    public final Right right;

    public Pair(Left left, Right right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) return false;
        Pair that = (Pair) o;
        return Objects.equals(this.left, that.left) && Objects.equals(this.right, that.right);
    }

    @Override
    public String toString() {
        return "(" + left + ", " + right + ")";
    }
}
