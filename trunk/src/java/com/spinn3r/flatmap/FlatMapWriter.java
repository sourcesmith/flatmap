package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Used to serialize a Map into a FlatMap capable of being mmap'd into memory
 * for high performance key lookup with a FlatMap.
 */
public class FlatMapWriter {

    /**
     * Use the first four bytes to denote the file version.
     */
    public static final byte[] MAGIC = "FM01".getBytes();

    public static final int TYPE_INT      = 1;
    public static final int TYPE_BYTE     = 2;
    public static final int TYPE_LONG     = 3;
    public static final int TYPE_DOUBLE   = 4;
    public static final int TYPE_FLOAT    = 5;

    public static final TypeHandler[] TYPE_HANDLER_LOOKUP = new TypeHandler[ 10 ];

    static {
        TYPE_HANDLER_LOOKUP[TYPE_INT]      = new IntegerTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_BYTE]     = new ByteTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_LONG]     = new LongTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_DOUBLE]   = new DoubleTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_FLOAT]    = new FloatTypeHandler();
    }
    
    public void write( Map map, OutputStream out ) throws IOException {

        out.write( MAGIC );

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

        TypeHandler key_type_handler   = TYPE_HANDLER_LOOKUP[key_type];
        TypeHandler value_type_handler = TYPE_HANDLER_LOOKUP[value_type];

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

    public static TypeHandler getTypeHandler( int key_type ) {
        return TYPE_HANDLER_LOOKUP[key_type];
    }

    public static byte[] toByteArray( int v ) {
        return new IntegerTypeHandler().toByteArray( new Integer( v ) );
    }

    public static int toInt( byte[] data ) {
        Integer i = (Integer)new IntegerTypeHandler().toValue( data );
        return i.intValue();
    }

    public static int typeOf( Object o ) {

        if ( o instanceof Integer )
            return TYPE_INT;

        if ( o instanceof Long )
            return TYPE_LONG;

        //FIXME: needs other types.....
        
        return -1;
        
    }

}

interface TypeHandler {

    public byte[] toByteArray( Object o );

    public Object toValue( byte[] data );

    /**
     * Size of this type (in bytes).
     */
    public int sizeOf();
    
}

class IntegerTypeHandler implements TypeHandler {

    public byte[] toByteArray( Object o ) {

        Integer i = (Integer) o;
        int value = i.intValue();
        
        byte[] b = new byte[4];

        b[0] = (byte)((value >> 24) & 0xFF);
        b[1] = (byte)((value >> 16) & 0xFF);
        b[2] = (byte)((value >> 8) & 0xFF);
        b[3] = (byte)((value >> 0) & 0xFF);

        return b;

    }

    public int toValueAsPrimitive( byte[] b ) {

        int v = (((((int) b[3]) & 0xFF) << 32) +
                 ((((int) b[2]) & 0xFF) << 40) +
                 ((((int) b[1]) & 0xFF) << 48) +
                 ((((int) b[0]) & 0xFF) << 56));

        return v;

    }

    public Object toValue( byte[] b ) {

        return new Integer( toValueAsPrimitive( b ) );

    }

    public int sizeOf() {
        return 4;
    }
}

class LongTypeHandler implements TypeHandler {

    public byte[] toByteArray( Object o ) {

        Long l = (Long) o;
        long value = l.longValue();

        byte[] b = new byte[8];

        b[0] = (byte)((value >> 56) & 0xFF);
        b[1] = (byte)((value >> 48) & 0xFF);
        b[2] = (byte)((value >> 40) & 0xFF);
        b[3] = (byte)((value >> 32) & 0xFF);

        b[4] = (byte)((value >> 24) & 0xFF);
        b[5] = (byte)((value >> 16) & 0xFF);
        b[6] = (byte)((value >> 8) & 0xFF);
        b[7] = (byte)((value >> 0) & 0xFF);

        return b;
        
    }

    public long toValueAsPrimitive( byte[] b ) {

        long v =  ((((long) b[7]) & 0xFF) +
                  ((((long) b[6]) & 0xFF) << 8) +
                  ((((long) b[5]) & 0xFF) << 16) +
                  ((((long) b[4]) & 0xFF) << 24) +
                  ((((long) b[3]) & 0xFF) << 32) +
                  ((((long) b[2]) & 0xFF) << 40) +
                  ((((long) b[1]) & 0xFF) << 48) +
                  ((((long) b[0]) & 0xFF) << 56));

        return v;
        
    }
    
    public Object toValue( byte[] b ) {

        return new Long( toValueAsPrimitive( b ) );

    }

    public int sizeOf() {
        return 8;
    }
}

class DoubleTypeHandler extends LongTypeHandler {

    public byte[] toByteArray( Object o ) {

        Double d = (Double) o;
        return super.toByteArray( Double.doubleToRawLongBits( d.doubleValue() ) );
        
    }

    public Object toValue( byte[] b ) {
        return new Double( Double.longBitsToDouble( super.toValueAsPrimitive( b ) ) );
    }
    
}

class FloatTypeHandler extends IntegerTypeHandler {

    public byte[] toByteArray( Object o ) {

        Float f = (Float) o;
        return super.toByteArray( Float.floatToRawIntBits( f.floatValue() ) );
        
    }

    public Object toValue( byte[] b ) {
        return new Float( Float.intBitsToFloat( super.toValueAsPrimitive( b ) ) );
    }
    
}

class ByteTypeHandler implements TypeHandler {

    public byte[] toByteArray( Object o ) {

        Byte v = (Byte) o;

        byte data[] = new byte[1];
        data[0] = v.byteValue();
        
        return data;
        
    }

    public Object toValue( byte[] b ) {
        return new Byte( b[0] );
    }

    public int sizeOf() {
        return 1;
    }
    
}

// http://java.sun.com/docs/books/tutorial/java/nutsandbolts/datatypes.html

//short
//boolean
//char

