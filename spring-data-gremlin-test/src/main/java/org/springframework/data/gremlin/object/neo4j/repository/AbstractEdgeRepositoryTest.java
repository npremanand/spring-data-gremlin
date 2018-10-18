package org.springframework.data.gremlin.object.neo4j.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.gremlin.object.neo4j.domain.Likes;
import org.springframework.data.gremlin.object.neo4j.domain.Located;
import org.springframework.data.gremlin.object.neo4j.domain.Location;

@SuppressWarnings("SpringJavaAutowiringInspection")
//@FixMethodOrder(MethodSorters.DEFAULT)
public abstract class AbstractEdgeRepositoryTest extends BaseRepositoryTest {

    @Autowired
    protected LikesRepository likesRepository;

    @Autowired
    protected LocatedRepository locatedRepository;

    @Test
    public void should_save_simple_edge() throws Exception {
        assertEquals(1, countObjects(likesRepository.findAll()));

        Likes likes = new Likes(lara, graham);
        lara.getLikes().add(likes);
        likesRepository.save(likes);

        assertEquals(2, countObjects(likesRepository.findAll()));
    }

	@Test
    public void should_findAll_Located() throws Exception {
        List<Located> located = new ArrayList<Located>();

        CollectionUtils.addAll(located, locatedRepository.findAll());
        assertNotNull(located);
        assertEquals(6, located.size());

        for (Located locate : located) {
            Assert.assertNotNull(locate.getLocation());
            Assert.assertNotNull(locate.getPerson());
        }
    }

    @Test
    public void should_deleteAll_Located() throws Exception {
        List<Located> located = new ArrayList<Located>();

        CollectionUtils.addAll(located, locatedRepository.findAll());
        assertEquals(6, located.size());
        located.clear();

        locatedRepository.deleteAll();

        CollectionUtils.addAll(located, locatedRepository.findAll());
        assertEquals(0, located.size());
    }

    @Test
    public void should_save_edge() throws Exception {
        Located located = new Located(new Date(), graham, locationRepository.save(new Location(35, 165)));
        graham.getLocations().add(located);
        locatedRepository.save(located);

        List<Located> newLocated = new ArrayList<Located>();
        CollectionUtils.addAll(newLocated, locatedRepository.findAll());
        assertEquals(7, newLocated.size());

    }

    @Test
    public void should_find_by_referenced() throws Exception {

        Likes likes = new Likes(graham, lara);
        likesRepository.save(likes);

        Iterable<Likes> all = likesRepository.findAll();
        Iterable<Likes> found = likesRepository.findByPerson1_FirstName("Graham");

        Collection<Likes> disjunction = CollectionUtils.disjunction(all, found);
        assertEquals(0, disjunction.size());
    }

    @Test
    public void should_find_by_query() throws Exception {

        Likes likes = new Likes(lara, graham);
        likesRepository.save(likes);

        Iterator<Likes> query = likesRepository.findByLiking("Lara", "Graham").iterator();
        assertTrue(query.hasNext());
        assertEquals(likes, query.next());
        assertFalse(query.hasNext());
    }
}
