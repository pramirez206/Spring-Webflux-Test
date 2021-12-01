package com.example.Spring.Webflux.Test;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class UppercaseConverterTest {

    final TestPublisher<String> testPublisher = TestPublisher.create();

    @Test
    void testUpperCase() {
        UppercaseConverter uppercaseConverter = new UppercaseConverter(testPublisher.flux());

        StepVerifier.create(uppercaseConverter.getUpperCase())
                .then(() -> testPublisher.emit("datos", "GeNeRaDoS", "Sofka"))
                .expectNext("DATOS", "GENERADOS", "SOFKA")
                .verifyComplete();
    }

}