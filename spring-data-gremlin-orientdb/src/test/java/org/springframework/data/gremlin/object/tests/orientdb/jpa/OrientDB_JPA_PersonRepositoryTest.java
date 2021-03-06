package org.springframework.data.gremlin.object.tests.orientdb.jpa;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.gremlin.object.jpa.domain.Person;
import org.springframework.data.gremlin.object.jpa.repository.AbstractPersonRepositoryTest;
import org.springframework.data.gremlin.object.jpa.repository.NativePersonRepository;
import org.springframework.test.context.ContextConfiguration;

import java.util.Iterator;

/**
 * Created by gman on 24/06/15.
 */
@ContextConfiguration(classes = OrientDB_JPA_TestConfiguration.class)
@SuppressWarnings("SpringJavaAutowiringInspection")
public class OrientDB_JPA_PersonRepositoryTest extends AbstractPersonRepositoryTest {

    @Autowired
    protected NativePersonRepository nativePersonRepository;

    @Test
    public void testDeleteAllExcept() throws Exception {
        int count = nativePersonRepository.deleteAllExceptUser("Lara");
        Assert.assertEquals(4, count);

        Iterable<Person> persons = repository.findAll();
        Assert.assertNotNull(persons);
        Iterator<Person> iterator = persons.iterator();
        Assert.assertNotNull(iterator);
        Assert.assertNotNull(iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }


    @Test
    public void findPeopleNear() throws Exception {
        Page<Person> page = nativePersonRepository.findNear(-33, 151, 50, PageRequest.of(0, 10));
        Assert.assertEquals(1, page.getTotalElements());

        Person person = page.iterator().next();
        Assert.assertNotNull(person);
        Assert.assertEquals("Graham", person.getFirstName());
        Assert.assertNotNull(person.getLocations());
    }
}
