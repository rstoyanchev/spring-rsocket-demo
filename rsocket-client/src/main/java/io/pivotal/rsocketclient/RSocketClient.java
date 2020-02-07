package io.pivotal.rsocketclient;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.pivotal.rsocketclient.data.Message;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

@Slf4j
@ShellComponent
public class RSocketClient {

    private final RSocketRequester rsocketRequester;
    private static final String ORIGIN = "Client";
    private static final String RR = "Request-Response";
    private static final String FAF = "Fire-And-Forget";
    private static final String STREAM = "Stream";
    private static final String CHANNEL = "Channel";

    private final Map<Integer, Disposable> streams = new HashMap<>();

    private final AtomicInteger index = new AtomicInteger();


    @Autowired
    public RSocketClient(RSocketRequester.Builder rsocketRequesterBuilder) {
        this.rsocketRequester = rsocketRequesterBuilder
                .connectTcp("localhost", 7000).block();
    }

    @ShellMethod("Send one request. One response will be printed.")
    public void requestResponse() {
        log.info("\nRequest-Response. Sending one request. Waiting for one response...");
        this.rsocketRequester
                .route("command")
                .data(new Message(ORIGIN, RR))
                .retrieveMono(Message.class)
                .subscribe(message -> log.info("Response received: {}", message));
    }

    @ShellMethod("Send one request. No response will be returned.")
    public void fireAndForget() {
        log.info("\nFire-And-Forget. Sending one request. Expect no response (check server)...");
        this.rsocketRequester
                .route("notify")
                .data(new Message(ORIGIN, FAF))
                .send()
                .subscribe()
                .dispose();
    }

    @ShellMethod("Send one request. Many responses (stream) will be printed.")
    public String stream() {
        log.info("\nRequest-Stream. Sending one request. Waiting for unlimited responses (Stop process to quit)...");
        return start(prefix -> this.rsocketRequester
                .route("stream")
                .data(new Message(ORIGIN, STREAM))
                .retrieveFlux(Message.class)
                .doOnNext(message -> log.info("{}: {}", prefix, message))
                .doOnCancel(() -> log.info("{} cancelled", prefix))
                .subscribe());
    }

    @ShellMethod("Stream ten requests. Ten responses (stream) will be printed.")
    public String channel(){
        log.info("\nChannel. Sending ten requests. Waiting for ten responses...");
        return start(prefix -> this.rsocketRequester
                .route("channel")
                .data(Flux.range(0,10).map(integer -> new Message(ORIGIN, CHANNEL, integer)), Message.class)
                .retrieveFlux(Message.class)
                .doOnNext(message -> log.info("{}: {}", prefix, message))
                .doOnCancel(() -> log.info("{}: cancelled", prefix))
                .subscribe());
    }

    @ShellMethod("Stop a previously started stream")
    public void stop(int streamId) {
        Disposable disposable = this.streams.remove(streamId);
        if (disposable != null) {
            disposable.dispose();
        }
    }

    private String start(Function<String, Disposable> factory) {
        int streamId = this.index.incrementAndGet();
        this.streams.put(streamId, factory.apply("\"[" + streamId + "]"));
        return "\nType \"Stop " + streamId + "\" to cancel\n";
    }

}
