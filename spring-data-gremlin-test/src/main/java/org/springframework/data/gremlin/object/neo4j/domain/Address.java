package org.springframework.data.gremlin.object.neo4j.domain;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.util.HashSet;
import java.util.Set;

@NodeEntity
public class Address {

    @GraphId
    private String id;

    private String country;

    private String city;

    private String street;

    @Relationship(type = "of_area")
    private Area area;

    @Relationship(type = "lives_at", direction = Relationship.INCOMING)
    private Set<Person> people;

    public Address() {}

    public Address(String country, String city, String street, Area area) {
        this.country = country;
        this.city = city;
        this.street = street;
        this.area = area;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public Area getArea() {
        return area;
    }

    public void setArea(Area area) {
        this.area = area;
    }

    public Set<Person> getPeople() {
        if (people == null) {
            people = new HashSet<Person>();
        }
        return people;
    }

    public void setPeople(Set<Person> people) {
        this.people = people;
    }
}
