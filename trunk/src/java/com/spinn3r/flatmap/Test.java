package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import static com.spinn3r.flatmap.TypeManager.*;

/**
 */
public class Test {
    
    static int total = 1000;
    static int start = -1000;
    static File file = null;
    static FlatMap<Long, Integer> fmap = null;

    public static void test1() {

        //FIXME: write boiler plate code to test ALL types.
        
        ShortTypeHandler shortTypeHandler = new ShortTypeHandler();

        short v = Short.MAX_VALUE;
        
        if ( ! shortTypeHandler.toValue( shortTypeHandler.toByteArray( v ) ).equals( new Short( v ) ) ) {
            throw new RuntimeException();
        }

    }

    public static void test2() throws Exception {

        //FIXME: test with long and [-100,100]
        
        Set<Integer> set = new TreeSet();

        int start = 1;
        int end   = 100;
        
        for( int i = start; i < end; ++i ) {
            set.add( i );
        }

        File file = new File( "test.fst" );
        
        FlatSetWriter writer = new FlatSetWriter();
        writer.write( set, file );

        FlatSet fset = new FlatSet( file );

        Iterator it = fset.iterator();
        while( it.hasNext() ) {
            System.out.printf( "%s\n", it.next() );
        }

        for( int i = start; i < end; ++i ) {
            if ( fset.contains( i ) )
                throw new Exception( "missing value: " + i );
        }

    }
    
    public static void main( String[] args ) throws Exception {

        test1();
        test2();
        
        System.out.printf( "Testing flat map\n" );

        file = new File( "test.fmp" );

        Map<Long, Integer> map = new TreeMap();

        for( long i = start; i < total; ++i ) {
            map.put( i, (int)i );
        }

        FlatMapWriter writer = new FlatMapWriter();
        writer.write( map, file );

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

