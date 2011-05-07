package info.chodakowski.esper.esperio.opentsdb;


public class Pair<A, B> {

	final private A first;
    final private B second;

    public Pair(final A first, final B second) {
        super();
        this.first = first;
        this.second = second;
    }

    
    public static Pair<String, String> pairOfStrings(final String first, final String second) {
    	return new Pair<String,String>(first, second);
    }
    
    public static <T> Pair<T, T> pairSameType(final T first, final T second) {
    	return new Pair<T,T>(first, second );
    }
    
    public int hashCode() {
        int hashFirst = first != null ? first.hashCode() : 0;
        int hashSecond = second != null ? second.hashCode() : 0;

        return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    public boolean equals(Object other) {
        if (other instanceof Pair) {
                Pair otherPair = (Pair) other;
                return 
                ((  this.first == otherPair.first ||
                        ( this.first != null && otherPair.first != null &&
                          this.first.equals(otherPair.first))) &&
                 (      this.second == otherPair.second ||
                        ( this.second != null && otherPair.second != null &&
                          this.second.equals(otherPair.second))) );
        }

        return false;
    }

    public String toString()
    { 
           return "(" + first + ", " + second + ")"; 
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

}