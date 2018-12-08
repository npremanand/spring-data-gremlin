package org.springframework.data.gremlin.object.neo4j.domain;


import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity
public class C {
    @Id @GeneratedValue
    public String id;
    
	@Property
	private String name;

    @Relationship(type = "D_C", direction=Relationship.INCOMING)
	private D d = null;

	public D getD() {
		return d;
	}

	public void setD(D d) {
		this.d = d;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}    

}
