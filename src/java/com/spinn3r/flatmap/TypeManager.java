package com.spinn3r.flatmap;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 */
public class TypeManager {

    private static final TypeHandler[] TYPE_HANDLER_LOOKUP = new TypeHandler[ 10 ];

    public static final int TYPE_INT      = 1;
    public static final int TYPE_BYTE     = 2;
    public static final int TYPE_LONG     = 3;
    public static final int TYPE_DOUBLE   = 4;
    public static final int TYPE_FLOAT    = 5;
    public static final int TYPE_BOOLEAN  = 6;
    public static final int TYPE_SHORT    = 7;

    static {
        TYPE_HANDLER_LOOKUP[TYPE_INT]      = new IntegerTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_BYTE]     = new ByteTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_LONG]     = new LongTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_DOUBLE]   = new DoubleTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_FLOAT]    = new FloatTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_BOOLEAN]  = new BooleanTypeHandler();
        TYPE_HANDLER_LOOKUP[TYPE_SHORT]    = new ShortTypeHandler();
    }

    public static TypeHandler lookupTypeHandler( int key_type ) {
        return TYPE_HANDLER_LOOKUP[key_type];
    }

    public static int typeOf( Object o ) {

        if ( o instanceof Integer )
            return TYPE_INT;

        if ( o instanceof Byte )
            return TYPE_BYTE;

        if ( o instanceof Long )
            return TYPE_LONG;

        if ( o instanceof Double )
            return TYPE_DOUBLE;

        if ( o instanceof Float )
            return TYPE_FLOAT;

        if ( o instanceof Boolean )
            return TYPE_BOOLEAN;

        if ( o instanceof Short )
            return TYPE_SHORT;

        throw new RuntimeException( "Type not supported: " + o.getClass().getName() );
        
    }

    public static byte[] toByteArray( int v ) {
        return new IntegerTypeHandler().toByteArray( new Integer( v ) );
    }

    public static int toInt( byte[] data ) {
        Integer i = (Integer)new IntegerTypeHandler().toValue( data );
        return i.intValue();
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

class BooleanTypeHandler implements TypeHandler {

    public byte[] toByteArray( Object o ) {

        Boolean v = (Boolean) o;

        byte data[] = new byte[1];
        data[0] = v.booleanValue() ? (byte)1 : (byte)0;
        
        return data;
        
    }

    public Object toValue( byte[] b ) {
        return new Boolean( b[0] == 1 );
    }

    public int sizeOf() {
        return 1;
    }
    
}

class ShortTypeHandler implements TypeHandler {

    public byte[] toByteArray( Object o ) {

        Short i = (Short) o;
        int value = i.shortValue();
        
        byte[] b = new byte[sizeOf()];

        b[0] = (byte)((value >> 8) & 0xFF);
        b[1] = (byte)((value >> 0) & 0xFF);

        return b;

    }

    public short toValueAsPrimitive( byte[] b ) {

        short v = (short)((((int) b[1]) & 0xFF) +
                         ((((int) b[0]) & 0xFF) << 8));

        return v;

    }

    public Object toValue( byte[] b ) {
        return new Short( toValueAsPrimitive( b ) );
    }

    public int sizeOf() {
        return 2;
    }
}

class CharTypeHandler extends ShortTypeHandler {

}

// http://java.sun.com/docs/books/tutorial/java/nutsandbolts/datatypes.html

//short
//char

