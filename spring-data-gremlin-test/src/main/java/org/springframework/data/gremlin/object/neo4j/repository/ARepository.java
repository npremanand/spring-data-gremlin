package org.springframework.data.gremlin.object.neo4j.repository;

import org.springframework.data.gremlin.object.neo4j.domain.A;
import org.springframework.data.gremlin.repository.GremlinRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ARepository extends GremlinRepository<A> {

}
