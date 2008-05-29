package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Provides the ability to compare byte arrays.
 *
 * @version $Id: $
 */
public class ByteArrayComparator implements Comparator {

    private TypeHandler typeHandler;
    
    public ByteArrayComparator( TypeHandler typeHandler ) {
        this.typeHandler = typeHandler;
    }
    
    public int compare( Object o1, Object o2 ) {

        return compare( typeHandler.toByteArray( o1 ),
                        typeHandler.toByteArray( o2 ) );

    }
    
    public static int compare( byte[] b1, byte[] b2 ) {

        if ( b1.length != b2.length ) {

            String msg = String.format( "differing lengths: %d vs %d", b1.length, b2.length );
            
            throw new RuntimeException( msg );
        }
        
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