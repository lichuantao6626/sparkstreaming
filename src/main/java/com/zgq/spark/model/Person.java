package com.zgq.spark.model;


import java.io.Serializable;

/**
 * 用户实例
 *
 * @classname Person
 * @date 2022年12月7日18:24:11
 */
public class Person implements Serializable {

    private String username;

    private Integer age;

    public Person(String username, Integer age) {
        this.username = username;
        this.age = age;
    }

    public Person() {
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "username='" + username + '\'' +
                ", age=" + age +
                '}';
    }
}
