package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

import java.nio.*;
import java.nio.channels.*;

import static com.spinn3r.flatmap.FlatMapWriter.*;

/**
 *
 */
public abstract class BaseFlatCollection  {

    public abstract int size();

    protected abstract TypeHandler getKeyTypeHandler();

    protected abstract byte[] getKeyFromPosition( int pos );

    /**
     * Perform a binary search of the key space in this flat map, return -1 if
     * the key was not found or the position of the key in the set/map if it was
     * found.
     *
     */
    protected int find( Object key ) {

        TypeHandler key_type_handler = getKeyTypeHandler();
                
        //make the key a byte array
        byte[] key_data = key_type_handler.toByteArray( key );

    	int low = 0;
    	int high = size() -1;

    	while (low <= high) {

    	    int mid = (low + high) >>> 1;
    	    byte[] midVal = getKeyFromPosition(mid);
            
    	    int cmp = ByteArrayComparator.compare( midVal, key_data );
            
    	    if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                // key found
                return mid; 
            }
            
    	}

    	return -1;  // key not found
        
    }

    protected ByteBuffer getByteBuffer( File file ) throws IOException {

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
        MappedByteBuffer bbuf = channel.map( FileChannel.MapMode.READ_ONLY, 0, (int)channel.size() );
        // force this buffer to load so that it doesn't load lazily.
        bbuf.load();

        return bbuf;
        
    }

    /**
     * Given an offset, and a length, fetch the given blocks.
     */
    protected byte[] get( ByteBuffer bbuf, int offset, int length ) {
        byte[] buff = new byte[length];

        //this is what ByteBuffer does internally anyway.
        for( int i = 0; i < length; ++i ) {
            buff[i] = bbuf.get( offset + i );
        }

        return buff;
    }

}

