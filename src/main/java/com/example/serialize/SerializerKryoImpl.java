package com.example.serialize;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;

public class SerializerKryoImpl implements ISerializer {
    
    private final KryoPool pool;
    
    public SerializerKryoImpl() {
        this(null, Collections.emptyMap());
    }
    
    public SerializerKryoImpl(@SuppressWarnings("rawtypes") Class<? extends Serializer> defaultSerializerType) {
        this(defaultSerializerType, Collections.emptyMap());
    }
    
    public SerializerKryoImpl(@SuppressWarnings("rawtypes") Class<? extends Serializer> defaultSerializerType,
            Map<Class<?>, Class<? extends Serializer<?>>> serializers) {
        this.pool = new KryoPool.Builder(() -> {
            final Kryo kryo = new Kryo();
            if (defaultSerializerType != null) {
                kryo.setDefaultSerializer(defaultSerializerType);  
            }
            for (Entry<Class<?>, Class<? extends Serializer<?>>> entry : serializers.entrySet()) {
                kryo.addDefaultSerializer(entry.getKey(), entry.getValue());
            }
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

    @Override
    public void write(OutputStream out, byte[] b) {
        try (Output output = new Output(out);) {
            output.write(b);
        }
    }
    
    @Override
    public void write(OutputStream out, byte[] b, long size) {
        try (Output output = new Output(out);) {
            output.write(b);
            output.writeLong(size);
        }
    }

    @Override
    public byte[] read(InputStream in, int bytes) {
        try (Input input = new Input(in);) {
            return input.readBytes(bytes);
        }
    }


}
