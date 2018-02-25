package com.example.serialize;

import java.io.InputStream;
import java.io.OutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;

public class SerializerKryoImpl implements ISerializer {
    
    private final KryoPool pool;
    
    public SerializerKryoImpl() {
        this.pool = new KryoPool.Builder(() -> {
            final Kryo kryo = new Kryo();
            return kryo;
        }).softReferences().build();
    }
    
    @Override
    public void write(OutputStream out, Object o) {
        try (Output output = new Output(out);) {
            this.pool.run(kryo -> {
                kryo.writeObject(output, o);
                return null;
            });
        }
    }
    
    @Override
    public <T> T read(InputStream in, Class<T> clazz) {
        try (Input input = new Input(in);) {
            return this.pool.run(kryo -> {
                return kryo.readObject(input, clazz);
            });
        }
    }
}
