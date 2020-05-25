package org.springframework.data.gremlin.object.neo4j.domain;

import org.neo4j.ogm.annotation.EndNode;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.RelationshipEntity;
import org.neo4j.ogm.annotation.StartNode;

import java.util.Date;

/**
 * Created by gman on 16/09/15.
 */
@RelationshipEntity
public class Likes {

    @Id
    private String id;

    private Date date = new Date();

    @StartNode
    private Person person1;

    @EndNode
    private Person person2;

    public Likes() {
    }

    public Likes(Person person1, Person person2) {
        this.person1 = person1;
        this.person2 = person2;
        person1.getLikes().add(this);
    }

    public Date getDate() {
        return date;
    }

    public Person getPerson1() {
        return person1;
    }

    public Person getPerson2() {
        return person2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof Likes)) {
            return false;
        }

        Likes likes = (Likes) o;

        return !(id != null ? !id.equals(likes.id) : likes.id != null);

    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
