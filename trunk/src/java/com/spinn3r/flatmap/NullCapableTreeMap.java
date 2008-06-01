package com.spinn3r.flatmap;

import java.util.*;


/**
 * 
 * http://feedblog.org/2008/06/01/java-treemap-and-hashmap-incompatibilities/
 * 
 * I will often use a TreeMap in place of a HashMap because while TreeMap is
 * theoretically lower (O(logN) vs O(1)) it is often much more efficient,
 * especially for larger maps.
 * 
 * The problem is that you can't just swap your map for TreeMap without
 * modifying code because you can't store null keys in TreeMap.
 * 
 * This code:
 * 
 * <code>map = new HashMap();
 * map.put( null, "bar" );
 * System.out.printf( "%s\n", map.get( null ) );
 * </code>
 * 
 * works fine and prints 'bar'.
 * 
 * This code:
 * 
 * <code>map = new TreeMap();
 * map.put( null, "bar" );
 * System.out.printf( "%s\n", map.get( null ) );</code>
 * 
 * ... will throw a null pointer exception.
 * 
 * This is true because the TreeMap Comparator doesn't support comparing null
 * values.  Why not?
 * 
 * I can pass in my own Comparator but now I need to remember this for every
 * instance of TreeMap.
 * 
 * Come to think of it.  I've often seen Map implementations slowing down
 * somewhat decent code.  The 2N rehash functionality of HashMap often means
 * that it can blow your memory footprint out of the water and crash your JVM.
 * 
 * If one were to use <a href="http://code.google.com/p/google-guice/">Guice</a>
 * to inject the dependency on the right Map implementation 3rd party libraries
 * would be able to swap their Map implementations out at runtime.
 * 
 * Further, if memory is your biggest problem, I think you might want to punt on
 * using the Java internal Map implementations and use <a
 * href="http://code.google.com/p/flatmap/">FlatMap</a> (even though it has some
 * limitations).
 * 
 */
public class NullCapableTreeMap extends TreeMap {

    public NullCapableTreeMap() {
        super( new NullCapableComparator() );
    }
    
}

class NullCapableComparator implements Comparator {
    
    public int compare( Object k1 , Object k2 ) {
        
        if ( k1 == null && k2 == null )
            return 0;
        else if ( k1 == null && k2 != null )
            return -1;
        else if ( k1 != null && k2 == null )
            return 1;
        
        return ((Comparable)k1).compareTo(k2); 
    }

}