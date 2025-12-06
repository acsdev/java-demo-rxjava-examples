package com.example;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.util.UUID;

import static com.example.Util.sleep;

public class RxJavaSubject {

    public static void main(String[] args) {
        PublishSubject<Integer> subject = PublishSubject.create();
        subject.subscribe(getSubscriber()); // Observer
        subject.onNext(1); // Notifier
        subject.onNext(2); // Notifier
        subject.onNext(3); // Notifier
        subject.subscribe(getSubscriber()); // Observer
        subject.onNext(4); // Notifier
        subject.onComplete();

        sleep(2_000L);

        // Output
        // subscribe FIRST_ID: 1
        // subscribe FIRST_ID: 3 (2 + 1)
        // subscribe FIRST_ID: 6 (3 + 3)
        // subscribe FIRST_ID: 10 ( 4 + 4 )
        // subscribe SECOND_ID: 4
        // Conclusion:
        //             The first Observer receives all four notifications
        //             BUT the second Observer, receives ONLY the last
    }

    private static Observer<Integer> getSubscriber() {
        return new Observer<>() {
            private final UUID id = UUID.randomUUID();

            private Integer value = 0;
            @Override
            public void onSubscribe(@NonNull Disposable d) {

            }

            @Override
            public void onNext(@NonNull Integer value) {
                this.value += value;
                System.out.println("subscribe " + id + ": " + this.value);
            }

            @Override
            public void onError(Throwable e) {
                System.out.println("error");
            }

            @Override
            public void onComplete() {
                System.out.println("Subscriber "+ id +" completed");
            }
        };
    }
}
