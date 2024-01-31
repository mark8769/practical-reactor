import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * In this chapter we are going to cover fundamentals of how to create a sequence. At the end of this
 * chapter we will tackle more complex methods like generate, create, push, and we will meet them again in following
 * chapters like Sinks and Backpressure.
 *
 * Read first:
 *
 * https://projectreactor.io/docs/core/release/reference/#which.create
 * https://projectreactor.io/docs/core/release/reference/#producing
 * https://projectreactor.io/docs/core/release/reference/#_simple_ways_to_create_a_flux_or_mono_and_subscribe_to_it
 *
 * Useful documentation:
 *
 * https://projectreactor.io/docs/core/release/reference/#which-operator
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html
 *
 * @author Stefan Dragisic
 */
public class c5_CreatingSequence {

    /**
     * Emit value that you already have.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#just-T-
    // Create a new Mono that emits the specified item, which is captured at instantiation time.
    @Test
    public void value_I_already_have_mono() {
        String valueIAlreadyHave = "value";
        //todo: change this line only
        Mono<String> valueIAlreadyHaveMono = Mono.just(valueIAlreadyHave);

        StepVerifier.create(valueIAlreadyHaveMono)
                    .expectNext("value")
                    .verifyComplete();
    }

    /**
     * Emit potentially null value that you already have.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#justOrEmpty-T-
    // Create a new Mono that emits the specified item if non null otherwise only emits onComplete.
    @Test
    public void potentially_null_mono() {
        String potentiallyNull = null;
        //todo change this line only
        Mono<String> potentiallyNullMono = Mono.justOrEmpty(potentiallyNull);

        StepVerifier.create(potentiallyNullMono)
                    .verifyComplete();
    }

    /**
     * Emit value from a optional.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#justOrEmpty-java.util.Optional-
    // Create a new Mono that emits the specified item if Optional.isPresent() otherwise only emits onComplete.
    @Test
    public void optional_value() {
        Optional<String> optionalValue = Optional.of("optional");
        //todo: change this line only
        Mono<String> optionalMono = Mono.justOrEmpty(optionalValue);

        StepVerifier.create(optionalMono)
                    .expectNext("optional")
                    .verifyComplete();
    }

    /**
     * Convert callable task to Mono.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#fromCallable-java.util.concurrent.Callable-
    // Create a Mono producing its value using the provided Callable. If the Callable resolves to null, the resulting Mono completes empty.
    @Test
    public void callable_counter() {
        AtomicInteger callableCounter = new AtomicInteger(0);
        Callable<Integer> callable = () -> {
            System.out.println("You are incrementing a counter via Callable!");
            return callableCounter.incrementAndGet();
        };

        //todo: change this line only
        Mono<Integer> callableCounterMono = Mono.fromCallable(callable);

        StepVerifier.create(callableCounterMono.repeat(2))
                    .expectNext(1, 2, 3)
                    .verifyComplete();
    }

    /**
     * Convert Future task to Mono.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#fromFuture-java.util.concurrent.CompletableFuture-
    // Create a Mono, producing its value using the provided CompletableFuture and cancelling the future if the Mono gets cancelled.
    @Test
    public void future_counter() {
        AtomicInteger futureCounter = new AtomicInteger(0);
        CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(() -> {
            System.out.println("You are incrementing a counter via Future!");
            return futureCounter.incrementAndGet();
        });
        // todo: change this line only
        Mono<Integer> futureCounterMono = Mono.fromFuture(completableFuture);

        StepVerifier.create(futureCounterMono)
                    .expectNext(1)
                    .verifyComplete();
    }

    /**
     * Convert Runnable task to Mono.
     */
    //todo: change this line only
    @Test
    public void runnable_counter() {
        AtomicInteger runnableCounter = new AtomicInteger(0);
        Runnable runnable = () -> {
            runnableCounter.incrementAndGet();
            System.out.println("You are incrementing a counter via Runnable!");
        };
        Mono<Integer> runnableMono = Mono.fromRunnable(runnable);

        StepVerifier.create(runnableMono.repeat(2))
                    .verifyComplete();

        Assertions.assertEquals(3, runnableCounter.get());
    }

    /**
     * Create Mono that emits no value but completes successfully.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#empty--
    @Test
    public void acknowledged() {
        //todo: change this line only
        Mono<String> acknowledged = Mono.empty();

        StepVerifier.create(acknowledged)
                    .verifyComplete();
    }

    /**
     * Create Mono that emits no value and never completes.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#never--
    @Test
    public void seen() {
        //todo: change this line only
        Mono<String> seen = Mono.never();

        StepVerifier.create(seen.timeout(Duration.ofSeconds(5)))
                    .expectSubscription()
                    .expectNoEvent(Duration.ofSeconds(4))
                    .verifyTimeout(Duration.ofSeconds(5));
    }

    /**
     * Create Mono that completes exceptionally with exception `IllegalStateException`.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html#error-java.lang.Throwable-
    @Test
    public void trouble_maker() {
        //todo: change this line
        Mono<String> trouble = Mono.error(new IllegalStateException());

        StepVerifier.create(trouble)
                    .expectError(IllegalStateException.class)
                    .verify();
    }

    /**
     * Create Flux that will emit all values from the array.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#fromArray-T:A-
    @Test
    public void from_array() {
        Integer[] array = {1, 2, 3, 4, 5};
        //todo: change this line only
        Flux<Integer> arrayFlux = Flux.fromArray(array);

        StepVerifier.create(arrayFlux)
                    .expectNext(1, 2, 3, 4, 5)
                    .verifyComplete();
    }

    /**
     * Create Flux that will emit all values from the list.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#fromIterable-T:A-
    @Test
    public void from_list() {
        List<String> list = Arrays.asList("1", "2", "3", "4", "5");
        //todo: change this line only
        Flux<String> listFlux = Flux.fromIterable(list);

        StepVerifier.create(listFlux)
                    .expectNext("1", "2", "3", "4", "5")
                    .verifyComplete();
    }

    /**
     * Create Flux that will emit all values from the stream.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#fromStream-T:A-
    @Test
    public void from_stream() {
        Stream<String> stream = Stream.of("5", "6", "7", "8", "9");
        //todo: change this line only
        Flux<String> streamFlux = Flux.fromStream(stream);

        StepVerifier.create(streamFlux)
                    .expectNext("5", "6", "7", "8", "9")
                    .verifyComplete();
    }

    /**
     * Create Flux that emits number incrementing numbers at interval of 1 second.
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#interval-java.time.Duration-
    @Test
    public void interval() {
        //todo: change this line only
        Flux<Long> interval = Flux.interval(Duration.ofSeconds(1));

        System.out.println("Interval: ");
        StepVerifier.create(interval.take(3).doOnNext(System.out::println))
                    .expectSubscription()
                    .expectNext(0L)
                    .expectNoEvent(Duration.ofMillis(900))
                    .expectNext(1L)
                    .expectNoEvent(Duration.ofMillis(900))
                    .expectNext(2L)
                    .verifyComplete();
    }

    /**
     * Create Flux that emits range of integers from [-5,5].
     */
    // https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#range-int-int-
    @Test
    public void range() {
        //todo: change this line only
        Flux<Integer> range = Flux.range(-5, 11);

        System.out.println("Range: ");
        StepVerifier.create(range.doOnNext(System.out::println))
                    .expectNext(-5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5)
                    .verifyComplete();
    }

    /**
     * Create Callable that increments the counter and returns the counter value, and then use `repeat()` operator to create Flux that emits
     * values from 0 to 10.
     */
    @Test
    public void repeat() {
        AtomicInteger counter = new AtomicInteger(0);

        Callable<Integer> callable = () -> {
            return counter.incrementAndGet();
        };
        //todo: change this line
        Flux<Integer> repeated = Mono
                .fromCallable(callable) // Start at 0 -> 1 -> 2
                .repeat(9); // Put 0 into flux -> Put 1 -> Put 2


        System.out.println("Repeat: ");
        StepVerifier.create(repeated.doOnNext(System.out::println))
                    .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                    .verifyComplete();
    }

    /**
     * Following example is just a basic usage of `generate,`create`,`push` sinks. We will learn how to use them in a
     * more complex scenarios when we tackle backpressure.
     * https://projectreactor.io/docs/core/release/reference/#_simple_ways_to_create_a_flux_or_mono_and_subscribe_to_it
     * Answer:
     * - What is difference between `generate` and `create`?
     *          GENERATE - Takes a generator function, SYNCHRONOUS and ONE-BY-ONE EMISSIONS (can only process 1 request at a time)
     *          CREATE - Advanced form of programmatic creation of a Flux, suitatble for multiple EMISSIONS (multiple threads)
     *
     * - What is difference between `create` and `push`?
     *          Create -
     *          Push - Middle ground between generate and create which is suitable for processing events from a single producer.
     *                  Can also be asynchronous and can manage backpressure. (Only one thread)
     *
     */
    @Test
    // https://projectreactor.io/docs/core/release/reference/#howtoReadMarbles
    public void generate_programmatically() {

        //todo: fix following code so it emits values from 0 to 5 and then completes
        AtomicInteger counter = new AtomicInteger(0);
        Flux<Integer> generateFlux = Flux.generate(sink -> {
            if (counter.get() > 5){
                sink.complete(); // Finish creating
            }
            sink.next(counter.getAndIncrement()); // Emit element
        });

        //------------------------------------------------------
        //todo: fix following code so it emits values from 0 to 5 and then completes
        Flux<Integer> createFlux = Flux.create(sink ->{
            for (int i=0; i<=5; i++){
                sink.next(i);
            }
            sink.complete();
        });

        //------------------------------------------------------
        //todo: fix following code so it emits values from 0 to 5 and then completes
        counter.set(0);
        Flux<Integer> pushFlux = Flux.push(sink -> {
            for (int i = 0; i <= 5; i++) {
                sink.next(i);
            }
            sink.complete();
        });

        StepVerifier.create(generateFlux)
                    .expectNext(0, 1, 2, 3, 4, 5)
                    .verifyComplete();

        StepVerifier.create(createFlux)
                    .expectNext(0, 1, 2, 3, 4, 5)
                    .verifyComplete();

        StepVerifier.create(pushFlux)
                    .expectNext(0, 1, 2, 3, 4, 5)
                    .verifyComplete();
    }

    /**
     * Something is wrong with the following code. Find the bug and fix it so test passes.
     */
    @Test
    public void multi_threaded_producer() {
        //todo: find a bug and fix it!
        Flux<Integer> producer = Flux.create(sink -> {
            for (int i = 0; i < 100; i++) {
                int finalI = i;
                // CREATE: Programmatically create a Flux with the capability of emitting multiple elements in a synchronous or asynchronous manner through the FluxSink API.
                // PUSH: Programmatically create a Flux with the capability of emitting multiple elements from a SINGLE-threaded producer through the FluxSink API.
                new Thread(() -> sink.next(finalI)).start(); //don't change this line! // THIS IS NOT SINGLE!!!!!!!!!!!!!!!! SADLY I LOOKED AT SOLUTION THEN READ API CLOSER...
            }
        });

        //do not change code below
        StepVerifier.create(producer
                                    .doOnNext(System.out::println)
                                    .take(100))
                    .expectNextCount(100)
                    .verifyComplete();
    }
}
