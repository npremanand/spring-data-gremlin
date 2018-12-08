package org.springframework.data.gremlin.object.neo4j.domain;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity
public class D {
	
	@Id @GeneratedValue
    public String id;

	@Property
	private String name;
    
    @Relationship(type = "D_C")
	private Set<C> cs = new HashSet<>();

	public String getName() {
		return name;
	}

	public void setName(String dName) {
		this.name = dName;
	}

	public Set<C> getCs() {
		return cs;
	}

	public void setCs(Set<C> cs) {
		this.cs = cs;
	}
}
