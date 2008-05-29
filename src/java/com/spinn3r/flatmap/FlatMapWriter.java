package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

import static com.spinn3r.flatmap.TypeManager.*;

/**
 * Used to serialize a Map into a FlatMap capable of being mmap'd into memory
 * for high performance key lookup with a FlatMap.
 */
public class FlatMapWriter {

    public void write( Map map, File file ) throws IOException {
        write( map, new FileOutputStream( file ) );        
    }

    public void write( Map map, OutputStream out ) throws IOException {

        out.write( FlatMap.MAGIC );

        //sort the keys:
        List keys = new ArrayList( map.keySet() );

        //write the number of items in the key.
        out.write( toByteArray( map.size() ) );

        //write constants for key/value pairs

        Object first_key     = keys.get( 0 );
        Object first_value   = map.get( first_key );
        int key_type         = typeOf( first_key );
        int value_type       = typeOf( first_value );
        
        out.write( toByteArray( key_type ) );
        out.write( toByteArray( value_type ) );

        TypeHandler key_type_handler   = lookupTypeHandler( key_type );
        TypeHandler value_type_handler = lookupTypeHandler( value_type );

        ByteArrayComparator comparator = new ByteArrayComparator( key_type_handler );
        
        Collections.sort( keys, comparator );

        //now dump the whole sorted list of keys and values into the output
        //stream:
        for( Object key : keys ) {
            Object value = map.get( key );
            out.write( key_type_handler.toByteArray( key ) );
            out.write( value_type_handler.toByteArray( value ) );
        }

        out.close();
        
    }

}

