package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

import static com.spinn3r.flatmap.TypeManager.*;

/**
 * Used to serialize a Set into a FlatSet capable of being mmap'd into memory
 * for high performance key lookup with a FlatMap.  For simple membership
 * queries a FlatSet is more efficient than a FlatMap by about one byte per key.
 */
public class FlatSetWriter {

    public void write( Set in, File file ) throws IOException {
        write( in, new FileOutputStream( file ) );
    }
    
    public void write( Set in, OutputStream out ) throws IOException {

        //4 bytes for magic number.
        out.write( FlatSet.MAGIC );

        List values = new ArrayList( in );

        //4 bytes for size
        out.write( toByteArray( in.size() ) );

        Object first     = values.get( 0 );
        int    type      = typeOf( first );

        //4 bytes for type
        out.write( toByteArray( type ) );

        TypeHandler type_handler = lookupTypeHandler( type );

        ByteArrayComparator comparator = new ByteArrayComparator( type_handler );
        
        Collections.sort( values, comparator );

        //now dump the whole sorted list of values and values into the output
        //stream:
        for( Object v : values ) {
            out.write( type_handler.toByteArray( v ) );
        }

        out.close();
        
    }

}

