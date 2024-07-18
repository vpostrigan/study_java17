package com.spock2__spockframework_org_2024

import spock.lang.Specification
import spock.lang.Subject

class ExampleSpecification extends Specification {

    void setupSpec() {
        // setup code that needs to be run once at the start
    }

    void setup() {
        // setup code that needs to be run before every test method
    }

    void cleanup() {
        // code that tears down things at the end of a test method
    }

    void cleanupSpec() {
        // code that tears down everything at the end when all tests have run
    }

    def "should be a simple assertion"() {
        expect:
        1 == 1
    }

    def "should demonstrate given-when-then0"() {
        when:
        int sides = new Polygon(4).numberOfSides

        then:
        sides == 4
    }

    def "should demonstrate given-when-then"() {
        given:
        def polygon = new Polygon(4)

        when:
        int sides = polygon.numberOfSides

        then:
        sides == 4
    }

    def "should expect Exceptions"() {
        when:
        new Polygon(0)

        then:
        thrown(TooFewSidesException/*.class*/)
    }

    def "should expect Exceptions2"() {
        when:
        new Polygon(0)

        then:
        def e = thrown(TooFewSidesException/*.class*/)
        e.numberOfSides == 0
    }

    def "should expect Exceptions3 #sides"() {
        when:
        new Polygon(sides)

        then:
        def e = thrown(TooFewSidesException/*.class*/)
        e.numberOfSides == sides

        where:
        sides << [-1, 0, 1]
    }

    def "should be able to create a polygon with #sides sides"() {
        when:
        def polygon = new Polygon(sides)

        then:
        polygon.numberOfSides == sides

        where:
        sides << [3, 4, 5, 8, 14]
    }

    def "should use data tables for calculating max"() {
        expect:
        Math.max(a, b) == max

        where:
        a | b | max
        1 | 3 | 3
        7 | 4 | 7
        0 | 0 | 0
    }

    def "should be able to mock a concrete class"() {
        given:
        Renderer renderer = Mock()
        // @Subject - which object is being tested
        @Subject
        def polygon = new Polygon(4, renderer)

        when:
        polygon.draw()

        // method drawLine was called 4 times
        then:
        4 * renderer.drawLine()
    }

    def "should be able to create a stub"() {
        given:
        Palette palette = Stub()
        palette.getPrimaryColour() >> Colour.Red
        @Subject
        def renderer = new Renderer(palette)

        expect:
        renderer.getForegroundColour() == Colour.Red
    }
/*
    def "should use a helper method"() {
        given:
        Renderer renderer = Mock()
        def shapeFactory = new ShapeFactory(renderer)

        when:
        def polygon = shapeFactory.createDefaultPolygon()

        then:
        with(polygon) {
            numberOfSides == 4
            renderer == renderer
        }
        verifyAll (polygon) {
            numberOfSides == 4
            renderer == renderer
        }

    }
*/
}
