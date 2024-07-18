package com.spock2__spockframework_org_2024;

public class TooFewSidesException extends Throwable {
    int numberOfSides;

    public TooFewSidesException(String s, int numberOfSides) {
        this.numberOfSides = numberOfSides;
    }

}
