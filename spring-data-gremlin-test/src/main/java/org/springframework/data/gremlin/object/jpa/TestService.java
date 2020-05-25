package org.springframework.data.gremlin.object.jpa;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.gremlin.object.jpa.domain.Address;
import org.springframework.data.gremlin.object.jpa.domain.Person;
import org.springframework.data.gremlin.object.jpa.repository.AddressRepository;
import org.springframework.data.gremlin.object.jpa.repository.NativePersonRepository;
import org.springframework.data.gremlin.object.jpa.repository.PersonRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by gman on 30/07/15.
 */
@Service
public class TestService {

    @Autowired
    private PersonRepository repository;

    @Autowired
    private AddressRepository addressRepository;

    @Autowired(required = false)
    private NativePersonRepository nativePersonRepository;

    @Transactional
    public void create(Person person) {
        repository.save(person);
    }

    @Transactional
    public void failCreate(Person person, Address address) {
        create(person);

        Person test = new Person("Graham", "Webber", address, true);
        repository.save(test);
        repository.findByAddress_Area_Name("asdf");
        repository.queryLastName("asdf", PageRequest.of(0, 2));
        if (nativePersonRepository != null) {
            nativePersonRepository.findNear(-33d, 151d, 50, PageRequest.of(0, 2));
        }
        addressRepository.save(address);
        throw new IllegalStateException();
    }

}
