package com.example.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.compress.utils.IOUtils;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.google.common.collect.ImmutableMap;


public class Main {

    static String path = "tmp";
    static byte[] magicBytes = {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6};
    
    static ISerializer normalSerializer = new SerializerKryoImpl();
    static ISerializer cmpSerializer = new SerializerKryoImpl(CompatibleFieldSerializer.class,
            ImmutableMap.of());
    
    public static void main(String[] args) {

        FooDto dto = new FooDto("foo", 888, 999);
        
        byte[] b = writeToBytes(normalSerializer, dto);
        byte[] bb = writeToBytes(cmpSerializer, dto);
        
        ByteBuffer byteBuf = ByteBuffer.allocate(b.length + bb.length + magicBytes.length);
        byteBuf.put(b);
        byteBuf.put(bb);
        byteBuf.put(magicBytes);
        byte[] totalBytes = byteBuf.array();
        
//        write(normalSerializer, path, totalBytes, bb.length);
        write(normalSerializer, path, dto);
        
//        FooDto res = read(normalSerializer, path, FooDto.class);
        FooDto res = read(path, FooDto.class);
        System.out.println(res);
    }

    private static void write(ISerializer serializer, String path, Object o) {
        try (FileOutputStream out = new FileOutputStream(new File(path))) {
            serializer.write(out, o);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static void write(ISerializer serializer, String path, byte[] b) {
        try (FileOutputStream out = new FileOutputStream(new File(path))) {
            serializer.write(out, b);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static void write(ISerializer serializer, String path, byte[] b, long size) {
        try (FileOutputStream out = new FileOutputStream(new File(path))) {
            serializer.write(out, b, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static byte[] writeToBytes(ISerializer serializer, Object o) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            serializer.write(out, o);
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    private static <T> T read(ISerializer serializer, String path, Class<T> clazz) {
        try (FileInputStream in = new FileInputStream(new File(path))) {
            return serializer.read(in, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static <T> T read(String path, Class<T> clazz) {
        try (FileInputStream in = new FileInputStream(new File(path));
                ByteArrayOutputStream bos = new ByteArrayOutputStream();) {
            IOUtils.copy(in, bos);
            byte[] b = bos.toByteArray();
            
            if (b.length <= 24) {
                System.out.println("This is FS");
                try (ByteArrayInputStream bis = new ByteArrayInputStream(b)) {
                    return normalSerializer.read(bis, clazz);                    
                }
            }
            
            byte[] mb = Arrays.copyOfRange(b, b.length - 24, b.length - 8);
            if (Arrays.equals(mb, magicBytes)) {
                System.out.println("This is CFS");
                byte[] sizeb = Arrays.copyOfRange(b, b.length - 8, b.length);
                long size = ByteBuffer.wrap(sizeb).getLong();
                
                byte[] bb = Arrays.copyOfRange(b, b.length - 24 - Long.valueOf(size).intValue(), b.length - 24);
                try (Input input = new Input(new ByteArrayInputStream(bb))) {
                    return cmpSerializer.read(input, clazz);
                }
            } else {
                System.out.println("This is FS");
                try (ByteArrayInputStream bis = new ByteArrayInputStream(b)) {
                    return normalSerializer.read(bis, clazz);                    
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
