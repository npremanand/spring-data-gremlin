package org.springframework.data.gremlin.object.neo4j.domain;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity
public class A {
	@Id @GeneratedValue
    public String id;
    
	@Property
	private String name;
    
    @Relationship(type = "A_B")
	private Set<B> bs = new HashSet<>();

	public Set<B> getBs() {
		return bs;
	}

	public void setBs(Set<B> bs) {
		this.bs = bs;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
