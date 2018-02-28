package com.example.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FooDtoSerializer extends Serializer<FooDto> {

    @Override
    public void write(Kryo kryo, Output output, FooDto object) {
        output.writeString(object.getName());
        output.writeInt(object.getId());
        output.writeInt(object.getId2());
        
        byte[] b = new byte[8];;
        output.write(b);
    }

    @Override
    public FooDto read(Kryo kryo, Input input, Class<FooDto> type) {
        String name = input.readString(); 
        int id = input.readInt();
        int id2 = input.readInt();

        byte[] b = input.readBytes(8);
        return new FooDto(name, id, id2);
    }

}
