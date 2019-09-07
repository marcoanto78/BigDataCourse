package group;

class Person {
    public  String name;
    public  Integer SIN = 0;
    public  Integer age;


    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public Person() {

    }


    public String getAgeGroup() {

        String ageGroup = "";
        if (age >= 0 && age <= 14) {
            ageGroup = "child";
        }

        if (age >= 15 && age <= 24) {
            ageGroup = "youth";
        }

        if (age >= 25 && age <= 64) {
            ageGroup = "adult";
        }

        if (age >= 65) {
            ageGroup = "senior";
        }
        return ageGroup;
    }
}

