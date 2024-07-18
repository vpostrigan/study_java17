package com.spock2__spockframework_org_2024;

public class Polygon {
    private final int numberOfSides;
    private Renderer renderer;

    public Polygon(int numberOfSides) throws TooFewSidesException {
        this(numberOfSides, new Renderer(new Palette()));
    }

    public Polygon(int numberOfSides, Renderer renderer) throws TooFewSidesException {
        if (numberOfSides < 2) {
            throw new TooFewSidesException("You can't have fewer than 3 sides for a polygon", numberOfSides);
        }
        this.numberOfSides = numberOfSides;
        this.renderer = renderer;
    }

    public void draw() {
        for (int i = 0; i < numberOfSides; i++) {
            renderer.drawLine();
        }
    }

}
