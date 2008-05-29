package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

import java.nio.*;
import java.nio.channels.*;

import static com.spinn3r.flatmap.FlatMapWriter.*;
import static com.spinn3r.flatmap.TypeManager.*;

/**
 */
public class FlatSet<E> extends BaseFlatCollection {

    /**
     * Use the first four bytes to denote the file version.
     */
    public static final byte[] MAGIC = "FS01".getBytes();

    // 4 bytes for header
    // 4 bytes for count of items
    // 4 bytes for key type
    public static final int OFFSET = 12;

    int size        = 0;

    int key_type    = -1;

    /**
     * Internal backed buffer for our data.  
     *
     */
    ByteBuffer bbuf = null;

    /**
     * Handler for metadata round keys.
     */
    TypeHandler key_type_handler    = null;

    public FlatSet( File file ) throws IOException {

        bbuf = getByteBuffer( file );

        byte[] magic = new byte[MAGIC.length];

        magic               = get( bbuf, 0, magic.length );
        size                = toInt( get( bbuf, 4, 4 ) );
        key_type            = toInt( get( bbuf, 8, 4 ) );

        key_type_handler    = lookupTypeHandler( key_type );
        
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public Iterator<E> iterator() {
        return new FlatSetIterator( this );
    }
    
    /**
     * Main method for using a FlatSet.  Most FlatSets are used for membership
     * queries.
     */
    public boolean contains( E val ) {

        return find( val ) >= 0;
        
    }

    protected TypeHandler getKeyTypeHandler() {
        return key_type_handler;
    }
    
    protected byte[] getKeyFromPosition( int pos ) {

        int offset = OFFSET + (pos * key_type_handler.sizeOf());
        
        return get( bbuf, offset, key_type_handler.sizeOf() );
        
    }

}

class FlatSetIterator<E> implements Iterator {

    FlatSet fset = null;

    int idx = 0;
    
    public FlatSetIterator( FlatSet fset ) {
        this.fset = fset;
    }

    public E next() {

        byte[] b = fset.getKeyFromPosition( idx );
        TypeHandler th = fset.getKeyTypeHandler();
        
        E result = (E)th.toValue( b ); 

        ++idx;

        return result;
        
    }
    
    public boolean hasNext() {
        return idx < fset.size();
    }

    public void remove() {
        throw new RuntimeException( "read only" );
    }
    
}