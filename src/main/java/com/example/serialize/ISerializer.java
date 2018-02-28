package com.example.serialize;

import java.io.InputStream;
import java.io.OutputStream;

public interface ISerializer {
    
    void write(OutputStream out, Object o);
    
    <T> T read(InputStream in, Class<T> clazz);
    
    void write(OutputStream out, byte[] b);
    
    byte[] read(InputStream in, int bytes);

    default void write(OutputStream out, byte[] b, long size) {
    }
    
}
