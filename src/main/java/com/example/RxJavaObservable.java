package com.example;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableSource;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.functions.Supplier;
import io.reactivex.rxjava3.observables.ConnectableObservable;

import java.util.concurrent.TimeUnit;

import static com.example.Util.sleep;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class RxJavaObservable {

    public static void main(String[] args) {
        example_Using();

        sleep(3_000);
    }

    private static void example_TwoSubscriptions() {
        ConnectableObservable<Long> connectable
                = Observable.interval(200, TimeUnit.MILLISECONDS).publish();

        // register first observer
        connectable.subscribe(i -> System.out.println("Value: " + i));

        // register second observer
        connectable.subscribe(i -> System.out.println("Double: " + i * 2));

        // wait until connect to start processing
        sleep(2_000L);
        // start processing
        connectable.connect();

    }

    private static void example_Using() {

        // create a resources
        Supplier<String> resourceSupplier = () -> "TABARNAK";

        // creates an observable that will emit event from the resource
        Function<String, ObservableSource<? extends Integer>> sourceSupplier = r ->
                Observable.create(o -> {
                    for (Character c : r.toCharArray()) {
                        int ascii = (int) c;
                        o.onNext(ascii);
                    }
                    o.onComplete();
                });

        Consumer<String> resourceCleanup = r -> System.out.println("Disposed: " + r);
        Observable<Integer> values = Observable.using(
                resourceSupplier,
                sourceSupplier,
                resourceCleanup
        );

        StringBuffer buffer = new StringBuffer();
        // create an Observer / Subscriber to handle the events
        // each time thet Observable executes no.onNext(c);
        // this subscribe will receive and handle the message
        values.subscribe(v -> buffer.append(v).append(".") );

        System.out.println("Result: " + buffer); // 09.121.95.114.101.115.111.117.114.99.101. In ASCII
    }
}
