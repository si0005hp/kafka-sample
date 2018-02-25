package com.example.serialize;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        String path = "tmp";
        ISerializer serializer = new SerializerStdImpl();
        FooDto dto = new FooDto("foo");
        
        write(serializer, path, dto);
        FooDto res = read(serializer, path, FooDto.class);
        System.out.println(res);
    }

    private static void write(ISerializer serializer, String path, Object o) {
        try (FileOutputStream out = new FileOutputStream(new File(path))) {
            serializer.write(out, o);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static <T> T read(ISerializer serializer, String path, Class<T> clazz) {
        try (FileInputStream in = new FileInputStream(new File(path))) {
            return serializer.read(in, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
