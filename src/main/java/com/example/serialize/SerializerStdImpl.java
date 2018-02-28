package com.example.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class SerializerStdImpl implements ISerializer {
    
    @Override
    public void write(OutputStream out, Object o) {
        try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(o);
            oos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public <T> T read(InputStream in, Class<T> clazz) {
        try (ObjectInputStream ois = new ObjectInputStream(in)) {
            return clazz.cast(ois.readObject());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(OutputStream out, byte[] b) {
    }

    @Override
    public byte[] read(InputStream in, int bytes) {
        return null;
    }

 
}
