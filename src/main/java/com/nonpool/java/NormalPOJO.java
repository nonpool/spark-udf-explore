package com.nonpool.java;

public class NormalPOJO {

    public NormalPOJO(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public NormalPOJO() {
    }

    private String name;
    private Integer age;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
