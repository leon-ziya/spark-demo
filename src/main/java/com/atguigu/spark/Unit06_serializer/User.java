package com.atguigu.spark.Unit06_serializer;

import java.io.Serializable;

/**
 * @author 姜来
 * @ClassName User.java
 * @createTime 2022年12月21日 10:24:00
 */
public class User {
    private String name;
    private Integer age;
    private String school;

    public User() {
    }

    public User(String name, Integer age, String school) {
        this.name = name;
        this.age = age;
        this.school = school;
    }

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

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", school='" + school + '\'' +
                '}';
    }
}
