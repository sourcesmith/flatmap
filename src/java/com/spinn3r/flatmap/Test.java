package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

/**
 */
public class Test {

    //FIXME: test for size

    //FIXME: clean up code.
    //FIXME: implement Map

    //FIXME: the double and float code won't work because I NEED to figure out
    //how to sort these values.

    
    static int total = 1000;
    static int start = -1000;
    static File file = null;
    static FlatMap<Long, Integer> fmap = null;
    
    public static void main( String[] args ) throws Exception {

        byte b = -13;
        int v = (int)b;

        System.out.println( " FIXME: (debug): v: " + v );
        
        System.out.printf( "Testing flat map\n" );

        file = new File( "test.fmap" );
        FileOutputStream fos = new FileOutputStream( file );

        Map<Long, Integer> map = new TreeMap();

        for( long i = start; i < total; ++i ) {
            map.put( i, (int)i );
        }

        FlatMapWriter writer = new FlatMapWriter();
        writer.write( map, fos );
        fos.flush();
        fos.close();

        fmap = new FlatMap( Test.file );

        for( int i = 0; i < 100; ++i ) {
            new TestThread().start();
        }
        
    }
   
}

class TestThread extends Thread {

    public void run() {

        try {

            //if ( fmap.size != total )
            //    throw new Exception( "Wrong number of items: " + fmap.size );
            
            for( long i = Test.start; i < Test.total; ++i ) {
                if ( ! Test.fmap.get( i ).equals( new Integer( (int)i ) ) )
                    throw new RuntimeException("fail");
            }

        } catch ( Exception e ) {
            e.printStackTrace();
        }

        System.out.printf( "." );
        
    }

}

