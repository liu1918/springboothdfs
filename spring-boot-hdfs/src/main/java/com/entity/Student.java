package com.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Student implements Writable {
    private Integer id;
    private String name;

    public Student() {
        super();
    }

    public Student(Integer id, String name) {
        super();
        this.id = id;
        this.name = name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeChars(name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        name = in.readUTF();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
