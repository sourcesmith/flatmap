package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

import java.nio.*;
import java.nio.channels.*;

import static com.spinn3r.flatmap.FlatMapWriter.*;

/**
 *
 *
 * TODO: how do we implement variable width values?  Variable length keys are
 * fine as I can use truncated SHA1.  Variable length data is FINE but I have to
 * allocate 4 bytes per entry for the pointer to the data section.  I can get
 * around this problem by simply storing a DataPointer object in as the value
 * and adding this as an indirection to the underlying value.
 */
public class FlatMap<K,V> extends ReadOnlyMap implements Map {

    // 4 bytes for header
    // 4 bytes for count of items
    // 4 bytes for key type
    // 4 bytes for value type
    public static final int OFFSET = 16;

    int size        = 0;

    int key_width   = -1;
    int value_width = -1;

    int key_type    = -1;
    int value_type  = -1;

    //note, for fully in-memory buffers we can call ByteByffer.wrap()
    MappedByteBuffer bbuf = null;

    TypeHandler key_type_handler    = null;
    TypeHandler value_type_handler  = null;

    public FlatMap( File file ) throws IOException {

        FileInputStream in = new FileInputStream( file );
        FileChannel channel = in.getChannel();

        /*
         * http://en.wikipedia.org/wiki/Mmap
         * 
         * "In computing, mmap is a POSIX-compliant Unix system call that maps
         * files or devices into memory. It is a method of memory-mapped file
         * I/O. It naturally implements demand paging, because initially file
         * contents are not entirely read from disk and don't use physical RAM at
         * all. The actual reads from disk are done just in time.
         */
        bbuf = channel.map( FileChannel.MapMode.READ_ONLY, 0, (int)channel.size() );
        // force this buffer to load so that it doesn't load lazily.
        bbuf.load();

        System.out.println( " FIXME: (debug): " + bbuf.getClass() );
        
        byte[] magic = new byte[MAGIC.length];
        magic = get( 0, magic.length );

        size = toInt( get( 4, 4 ) );

        key_type    = toInt( get( 8, 4 ) );
        value_type  = toInt( get( 12, 4 ) );

        key_type_handler    = getTypeHandler( key_type );
        value_type_handler  = getTypeHandler( value_type );
        
    }

    /**
     * Given an offset, and a length, fetch the given blocks.
     */
    private byte[] get( int offset, int length ) {
        byte[] buff = new byte[length];

        //this is what ByteBuffer does internally anyway.
        for( int i = 0; i < length; ++i ) {
            buff[i] = bbuf.get( offset + i );
        }

        return buff;
    }

    private byte[] getKeyFromPosition( int pos ) {

        int offset = OFFSET +
            (pos * (key_type_handler.sizeOf() + value_type_handler.sizeOf()));
        
        return get( offset, key_type_handler.sizeOf() );
    }

    private V getValueFromPosition( int pos ) {

        int offset = OFFSET +
            (pos * (key_type_handler.sizeOf() + value_type_handler.sizeOf()));
        offset += key_type_handler.sizeOf();
        
        byte[] data = get( offset, value_type_handler.sizeOf() );

        return (V)value_type_handler.toValue( data );

    }
    
    /**
     * Perform a binary search of the key space in this flat map, return null if
     * the key was not found.
     *
     */
    public V get( K key ) {

        //make the key a byte array
        byte[] key_data = key_type_handler.toByteArray( key );

    	int low = 0;
    	int high = size -1;

    	while (low <= high) {

    	    int mid = (low + high) >>> 1;
    	    byte[] midVal = getKeyFromPosition(mid);
            
    	    int cmp = ByteArrayComparator.compare( midVal, key_data );
            
    	    if (cmp < 0)
                low = mid + 1;
    	    else if (cmp > 0)
                high = mid - 1;
    	    else
                return getValueFromPosition( mid ); // key found
    	}

    	return null;  // key not found
        
    }

    public public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean containsKey(Object key) {
        return get( key ) != null;
    }

    public boolean containsValue(Object value) {
        throw new RuntimeException( "not implemented yet" );
    }
    
    public Set<K> keySet() {
        throw new RuntimeException( "not implemented yet" );
    }

    public Collection<V> values() {
        throw new RuntimeException( "not implemented yet" );
    }

    public Set<Map.Entry<K, V>> entrySet() {
        throw new RuntimeException( "not implemented yet" );
    }
    
    private String format( byte[] b ) {

        String v = "";
        for( int i = 0; i < b.length; ++i ) {
            v += String.format( "%d ", b[i] );
        }

        return v;
        
    }
    
}

class ByteArrayComparator implements Comparator {

    private TypeHandler typeHandler;
    
    public ByteArrayComparator( TypeHandler typeHandler ) {
        this.typeHandler = typeHandler;
    }
    
    public int compare( Object o1, Object o2 ) {

        return compare( typeHandler.toByteArray( o1 ),
                        typeHandler.toByteArray( o2 ) );

    }
    
    public static int compare( byte[] b1, byte[] b2 ) {

        if ( b1.length != b2.length )
            throw new RuntimeException( String.format( "differing lengths: %d vs %d", b1.length, b2.length ) );
        
        for( int i = 0; i < b1.length; ++i ) {

            if ( b1[i] < b2[i] )
                return -1;

            if ( b1[i] > b2[i] )
                return 1;

            if ( b1[i] == b2[i] ) {

                //we're not done comparing yet.
                if ( i < b1.length - 1 )
                    continue;

                return 0;
                
            }
            
        }
        
        throw new RuntimeException();
        
    }

}