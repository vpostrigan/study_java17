package ch22.TimelineAnimation;

import java.security.SecureRandom;

import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.geometry.Bounds;
import javafx.scene.layout.Pane;
import javafx.scene.shape.Circle;
import javafx.util.Duration;

// TimelineAnimationController.java
// Bounce a circle around a window using a Timeline animation.

public class TimelineAnimationController {
    @FXML
    Circle c;
    @FXML
    Pane pane;

    public void initialize() {
        SecureRandom random = new SecureRandom();

        // define a timeline animation
        Timeline timelineAnimation = new Timeline(
                new KeyFrame(Duration.millis(10),
                        new EventHandler<>() {
                            int dx = 1 + random.nextInt(5);
                            int dy = 1 + random.nextInt(5);

                            // move the circle by the dx and dy amounts
                            @Override
                            public void handle(final ActionEvent e) {
                                c.setLayoutX(c.getLayoutX() + dx);
                                c.setLayoutY(c.getLayoutY() + dy);
                                Bounds bounds = pane.getBoundsInLocal();

                                if (hitRightOrLeftEdge(bounds)) {
                                    dx *= -1;
                                }

                                if (hitTopOrBottom(bounds)) {
                                    dy *= -1;
                                }
                            }
                        }
                )
        );

        // indicate that the timeline animation should run indefinitely
        timelineAnimation.setCycleCount(Timeline.INDEFINITE);
        timelineAnimation.play();
    }

    // determines whether the circle hit the left or right of the window
    private boolean hitRightOrLeftEdge(Bounds bounds) {
        return (c.getLayoutX() <= (bounds.getMinX() + c.getRadius())) ||
                (c.getLayoutX() >= (bounds.getMaxX() - c.getRadius()));
    }

    // determines whether the circle hit the top or bottom of the window
    private boolean hitTopOrBottom(Bounds bounds) {
        return (c.getLayoutY() <= (bounds.getMinY() + c.getRadius())) ||
                (c.getLayoutY() >= (bounds.getMaxY() - c.getRadius()));
    }

}
