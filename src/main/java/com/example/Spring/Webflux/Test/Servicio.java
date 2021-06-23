package com.example.Spring.Webflux.Test;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;


@Service
public class Servicio {

    public Mono<String> buscarUno() {
        return Mono.just("Pedro");
    }

    public Flux<String> buscarTodos() {
        return Flux.just("Pedro", "Maria", "Jesus", "Carmen");
    }

    public Flux<String> buscarTodosLento() {
        return Flux.just("Pedro", "Maria", "Jesus", "Carmen").delaySequence(Duration.ofSeconds(20));
    }

    public Flux<String> buscarTodosFiltro() {
        Flux<String> source = Flux.just("John", "Monica", "Mark", "Cloe", "Frank", "Casper", "Olivia", "Emily", "Cate")
                .filter(name -> name.length() == 4)
                .map(String::toUpperCase);

        Flux<String> error = source.concatWith(
                Mono.error(new IllegalArgumentException("Mensaje de Error"))
        );
        return error;
    }

    public Flux<Integer> emisorConParada() {
        Flux<Integer> source = Flux.<Integer>create(emitter -> {
            emitter.next(1);
            emitter.next(2);
            emitter.next(3);
            emitter.complete();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            emitter.next(4);
        }).filter(number -> number % 2 == 0);
        return source;
    }

}
