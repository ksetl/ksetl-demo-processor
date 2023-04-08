package org.ksetl.demo;

import javax.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
public class LegalEntityLookupService {

    private boolean allPass = false;

    public Optional<Integer> findLegalEntityId(String globalLegalEntityId, String targetSystemId) {
        if (allPass || globalLegalEntityId.length() < 5) {
            return Optional.of(globalLegalEntityId.length());
        }
        return Optional.empty();
    }

    public void setAllPass(boolean allPass) {
        this.allPass = allPass;
    }
}
