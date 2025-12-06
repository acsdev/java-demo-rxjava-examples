package com.example;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observables.ConnectableObservable;

import java.util.concurrent.TimeUnit;

import static com.example.Util.sleep;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class RxJavaObservable {

    public static void main(String[] args) {
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

        // kill main tread
        sleep(3_000L);
    }
}
