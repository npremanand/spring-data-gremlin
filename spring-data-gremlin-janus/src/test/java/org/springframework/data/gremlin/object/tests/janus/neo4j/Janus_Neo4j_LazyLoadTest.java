package org.springframework.data.gremlin.object.tests.janus.neo4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.gremlin.object.neo4j.domain.D;
import org.springframework.data.gremlin.object.neo4j.domain.C;
import org.springframework.data.gremlin.object.neo4j.domain.B;
import org.springframework.data.gremlin.object.neo4j.domain.Person;
import org.springframework.data.gremlin.object.neo4j.domain.A;
import org.springframework.data.gremlin.object.neo4j.repository.AbstractPersonRepositoryTest;
import org.springframework.data.gremlin.object.neo4j.repository.BRepository;
import org.springframework.data.gremlin.object.neo4j.repository.BaseRepositoryTest;
import org.springframework.data.gremlin.object.neo4j.repository.LikesRepository;
import org.springframework.data.gremlin.object.neo4j.repository.ARepository;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

@ContextConfiguration(classes = Janus_Neo4j_TestConfiguration.class)
@SuppressWarnings("SpringJavaAutowiringInspection")
@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@Rollback
@TestExecutionListeners(
        inheritListeners = false,
        listeners = { DependencyInjectionTestExecutionListener.class })
public class Janus_Neo4j_LazyLoadTest {
	
    @Autowired
    protected ARepository aRepository;

    @Autowired
    protected BRepository bRepository;

	@Test
	public void testSimpleSaveLoad() {
		A a = new A();
		a.setName("Alpha");
		a = aRepository.save(a);
		
		Optional<A> loadedA = aRepository.findById(a.id);
		assertTrue(loadedA.isPresent());
		assertEquals(a.id, loadedA.get().id);
		assertEquals("Alpha", loadedA.get().getName());
	}

	@Test
	public void testSaveCascade() {
		
		B b = new B();
		b.setName("Beta");
		
		C c = new C();
		c.setName("Charlie");
		b.getCs().add(c);
		D d = new D();
		d.setName("Delta");
		c.setD(d);
		d.getCs().add(c);
		
		b = bRepository.save(b);
		
		A a = new A();
		a.setName("Alpha");
		a.getBs().add(b);
		a = aRepository.save(a);
		
		// Test at vi kan finde den ved id
		Optional<A> loadedA = aRepository.findById(a.id);
		assertTrue(loadedA.isPresent());
		
		assertEquals(a.id, loadedA.get().id);
		assertEquals("Alpha", loadedA.get().getName());
		B loadedB = loadedA.get().getBs().iterator().next();
		assertEquals("Beta", loadedB.getName());
		assertEquals(1, loadedB.getCs().size());
		C loadedC = loadedB.getCs().iterator().next();
		assertEquals("Charlie", loadedC.getName());
		D loadedD = loadedC.getD();
		assertEquals("Delta", loadedD.getName());
	}
}
