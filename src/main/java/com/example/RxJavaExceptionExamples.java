package com.example;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class RxJavaExceptionExamples {

    public static void main(String[] args) {
//        System.out.println();
//        System.out.println();
//        System.out.println("1. ====================");
//        example_OnErrorReturn();
//
//        System.out.println();
//        System.out.println();
//        System.out.println("2. ====================");
//        example_OnErrorReturnItem();
//
//        System.out.println();
//        System.out.println();
//        System.out.println("3. ====================");
        example_DoOnError();
//
//        System.out.println();
//        System.out.println();
//        System.out.println("4. ====================");
//        example_OnErrorResumeNext();
//
//        System.out.println();
//        System.out.println();
//        System.out.println("5. ====================");
//        example_Retry();
//
//        System.out.println();
//        System.out.println();
//        System.out.println("6. ====================");
//        example_Retry_WithSingle();
//
//        System.out.println();
//        System.out.println();
//        System.out.println("7. ===================");
//        example_Continue_flow();

    }

    /**
     * onErrorReturn: Catches error and returns a default value
     * FLOW: STOP (Handles exception and returns a default value for the emitted observable which the error was thrown and stop the flow)
     */
    private static void example_OnErrorReturn() {
        Observable.just(1, 2, 0, 10)
                .map(num -> 10 / num)
                .onErrorReturn(error -> {
                    System.out.println("==> Error caught: " + error.getMessage());
                    return -1; // Default value
                })
                .subscribe(
                        result -> System.out.println("==> Result: " + result),
                        error -> System.out.println("==> Unhandled error: " + error),
                        () -> System.out.println("==> Complete!")
                );
    }

    /**
     * onErrorReturnItem: Simplified version of onErrorReturn
     * FLOW: STOP (Return a default value for the emitted observable which the error was thrown and stop the flow)
     */
    private static void example_OnErrorReturnItem() {

        Observable.just("A", "B", "C")
                .map(str -> {
                    if (str.equals("B")) {
                        throw new RuntimeException("Error on item B!");
                    }
                    return str + "-processed";
                })
                .onErrorReturnItem("DEFAULT-processed")
                .subscribe(
                        result -> System.out.println("==> Result: " + result),
                        error -> System.out.println("==> Error: " + error),
                        () -> System.out.println("==> Complete!")
                );

        System.out.println();
    }

    /**
     * doOnError: Executes action when error occurs (but doesn't handle the error)
     * FLOW: STOP
     */
    private static void example_DoOnError() {

        Observable.just(5, 10, 0, 20)
                .map(num -> 100 / num)
                .doOnError(error -> {
                    // Log or notification, but error continues propagating
                    System.out.println("==> [LOG] Error detected: " + error.getClass().getSimpleName());
                })
                // THAT can be ALSO used with return value for the emitted observable which the error was thrown
                // .onErrorReturnItem( 0 )
                // So, doOnError + onErrorReturnItem == onErrorReturn
                .subscribe(
                        result -> System.out.println("==> Result: " + result),
                        error -> System.out.println("==> Final error: " + error),
                        () -> System.out.println("==> Complete!")
                );
    }

    /**
     * onErrorResumeNext: REPLACES the Observable with error with another Observable
     * FLOW: Continues with new Observable ( NOT NEXT emission from the original Observable )
     */
    private static void example_OnErrorResumeNext() {

        Observable<String> fallbackData = Observable.just("C-NEW-processed", "D-NEW-processed");

        Observable.just("A", "B", "C")
                .map(str -> {
                    if (str.equals("B")) {
                        throw new RuntimeException("Error on item B!");
                    }
                    return str + "-processed";
                })
                .onErrorResumeNext(error -> {
                    System.out.println("==> Error detected, using fallback: " + error.getMessage());
                    return fallbackData;
                })
                .subscribe(
                        item -> System.out.println("==> Item: " + item),
                        error -> System.out.println("==> Error: " + error),
                        () -> System.out.println("==> Complete!")
                );
    }

    /**
     * retry: Retries on error
     * <p>
     * MAKES more sense for work with ONE ITEM
     * FLOW: STOP (After number of retries ended)
     */
    private static void example_Retry() {

        // Scenario A
        // For items to emit, but the fail occurs on the third
        // So, 3 retries multiply by 3 tries
        // Then, 12 executions (ENDs WITH ERROR )
        AtomicInteger executions = new AtomicInteger(0);
        Observable.just(5, 10, 0, 20)
                .map(num -> {
                    executions.incrementAndGet();
                    return 100 / num;
                })
                .retry(3) // RETRIES ALL EMISSION UNTIL ALL THE SUCCEEDED OR NUMBER OF RETRIES END
                .subscribe(
                        result -> System.out.println("==> " + result),
                        error -> System.out.println("==> Error after retries: " + error),
                        () -> System.out.println("==> Complete! With:" + executions.get() + "Executions")
                );

        // Scenario
        // For items to emit, but the fail occurs on the third (in the first try)
        // So, 2 retries (first with 3 executions, second with 4 executions)
        // Then, complete 7 (ENDs with COMPLETE)
        AtomicInteger executions2 = new AtomicInteger(0);
        Observable.just(5, 10, 0, 20)
                .map(num -> {
                    int value = executions2.incrementAndGet();
                    if (value == 6) {
                        // Second try on the third item
                        return -1;
                    }
                    return 60 / num;
                })
                .retry(3) // RETRIES ALL EMISSION UNTIL ALL THEY SUCCEED OR NUMBER OF RETRIES END
                .subscribe(
                        result -> System.out.println("==> " + result),
                        error -> System.out.println("==> Error after retries: " + error),
                        () -> System.out.println("==> Complete! With:" + executions2.get() + "Executions")
                );

    }

    /**
     * Retry: Retries on error
     * MAKES more sense for work with ONE ITEM
     * FLOW: STOP (After number of retries ended)
     */
    private static void example_Retry_WithSingle() {

        // Retries and NEVER succeed
        AtomicInteger attempt1 = new AtomicInteger(0);
        Single.just("A")
                .map(str -> {
                    attempt1.incrementAndGet();
                    throw new RuntimeException("Error on item!");
                })
                .retry(5)
                .subscribe(
                        result -> System.out.println("==> Finish "),
                        error -> System.out.println("==> Error after retries '" + attempt1.get() + "' : " + error)

                );

        // Retries and EVENTUALLY succeed
        AtomicInteger attempt2 = new AtomicInteger(0);
        Single.just("A")
                .map(str -> {
                    int value = attempt2.incrementAndGet();
                    if (value == 3) {
                        return "=WORKED=";
                    }
                    throw new RuntimeException("Error on item!");
                })
                .retry(5)
                .subscribe(
                        result -> System.out.println("==> Finish with result '" + result + "' number of tries: '" + attempt2.get() + "'"),
                        error -> System.out.println("==> Error after retries: " + error)

                );

    }

    /**
     * To continue normally the error needs to be handle inside the execution
     * So, it really never fails.
     * <p>
     * FLOW: Continues
     */
    private static void example_Continue_flow() {
        Observable.just(5, 10, 0, 20)
                .map(num -> {
                    try {
                        return 60 / num;
                    } catch (Exception ex) {
                        System.err.println(ex.getMessage());
                        return -1;
                    }
                })
                .retry(3) // RETRIES ALL EMISSION UNTIL ALL THEY SUCCEED OR NUMBER OF RETRIES END
                .subscribe(
                        result -> System.out.println("==> " + result),
                        error -> System.out.println("==> Error after retries: " + error),
                        () -> System.out.println("==> Complete!")
                );
    }
}