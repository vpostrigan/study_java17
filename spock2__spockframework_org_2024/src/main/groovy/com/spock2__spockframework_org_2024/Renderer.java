package com.spock2__spockframework_org_2024;

public class Renderer {
    private final Palette palette;

    public Renderer(Palette palette) {
        this.palette = palette;
    }

    public void drawLine() {

    }

    public Colour getForegroundColour() {
        return palette.getPrimaryColour();
    }

}
