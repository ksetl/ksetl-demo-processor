package org.ksetl.demo;

import javax.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
public class LegalEntityLookupService {

    public Optional<Integer> findLegalEntityId(String globalLegalEntityId, String targetSystemId) {
        return Optional.of(globalLegalEntityId.length());
    }

}
