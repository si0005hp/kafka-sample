package com.example.serialize;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@SuppressWarnings("serial")
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class FooDto implements Serializable {
    private String name;
    private int id;
    private int id2;
}
