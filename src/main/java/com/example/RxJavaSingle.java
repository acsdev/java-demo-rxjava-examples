package com.example;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;

import static com.example.Util.sleep;

public class RxJavaSingle {

    public static void main(String[] args) {
        String[] result = {""};
        Single<String> single = Single.just("Hello")
                .doOnSuccess(i -> result[0] += i)
                .doOnError(error -> {
                    throw new RuntimeException(error.getMessage());
                });
        single.subscribe(); // subscribes an empty consumer to ensure execution
        System.out.println(result[0]);
        // sleep(10_000L);
    }

}
